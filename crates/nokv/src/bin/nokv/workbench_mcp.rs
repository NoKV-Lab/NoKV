use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use nokv_agent::AgentToolDefinition;
use nokv_client::{
    decode_name_cursor, encode_name_cursor, is_artifact_write_conflict, is_metadata_not_found,
    ArtifactMetadata, ClientError, NoKvFsClient,
};
use nokv_meta::MetadError;
use nokv_object::ObjectStore;
use nokv_types::{FileType, PathMetadata};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{DEFAULT_MODE_DIR, DEFAULT_MODE_FILE};

pub const DEFAULT_WORKBENCH_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_WORKBENCH_FIND_LIMIT: usize = 50;
const MAX_WORKBENCH_FIND_LIMIT: usize = 100;
// Schema-declared caps mirroring the agent tool limits (nokv-agent lib.rs);
// the agent layer enforces them, these keep the advertised schemas honest.
const MAX_WORKBENCH_LIST_LIMIT: usize = 100;
const MAX_WORKBENCH_SEARCH_LIMIT: usize = 10;
const MAX_WORKBENCH_AGGREGATE_LIMIT: usize = 100;
const MAX_WORKBENCH_READ_LIMIT: usize = 300;
const MAX_WORKBENCH_GREP_LIMIT: usize = 300;
/// Mirror of the metadata server's grep pattern cap (nokv-meta
/// MAX_GREP_PATTERNS); enforced there, advertised and pre-checked here.
const MAX_WORKBENCH_GREP_PATTERNS: usize = 16;
/// Default snapshot lease when `workbench_snapshot` is called without `ttl_days`.
/// Leases express liveness, never archival importance; a week is long enough to
/// survive a handoff yet short enough that a forgotten pin still reaps.
const DEFAULT_SNAPSHOT_TTL_DAYS: u64 = 7;
/// Hard ceiling on the tool-set lease. Longer holds are the job of L1 named
/// refs (Phase 2) or the CLI, not a lease knob; requests beyond it are rejected
/// with that guidance so a lease never masquerades as durable retention.
const MAX_SNAPSHOT_TTL_DAYS: u64 = 90;
const MS_PER_DAY: u64 = 86_400_000;
/// Checkpoint registry file, relative to a workbench's `metadata` section.
/// Every mint/renew appends one JSON line here so checkpoints stay discoverable
/// after the tool response is gone (Phase-1 seed for L1 named refs).
const CHECKPOINT_REGISTRY_RELPATH: &str = "checkpoints.jsonl";
/// Retries after a lost artifact-write CAS; every attempt re-reads current state.
const WRITE_CONFLICT_RETRIES: usize = 5;
/// Linear backoff step between conflict retries. Zero-interval retries make N
/// synchronized writers replay the same race until the retry budget runs out;
/// a growing pause plus per-process jitter (see [`write_conflict_backoff`])
/// de-synchronizes them.
const WRITE_CONFLICT_BACKOFF_STEP_MS: u64 = 10;

/// Sleep before retry number `attempt` (1-based) of a conflicted write.
/// Linear `attempt * 10ms` backoff plus a 0-8ms desync offset derived from the
/// process id and the current clock nanoseconds, so two workbench processes
/// that lost the same race do not wake in lockstep (no rand dependency).
/// Wrap a write conflict that survived the whole retry budget into an
/// actionable error: the caller learns the write is safe to re-issue while
/// the original error text stays available for diagnosis.
fn write_conflict_exhausted(attempts: usize, err: impl fmt::Display) -> WorkbenchToolError {
    WorkbenchToolError::new(format!(
        "write conflicted with concurrent writers after {attempts} attempts; retry the call ({err})"
    ))
}

fn write_conflict_backoff(attempt: usize) {
    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|now| now.subsec_nanos() as u64)
        .unwrap_or(0)
        .wrapping_add(u64::from(std::process::id()))
        % 8;
    std::thread::sleep(std::time::Duration::from_millis(
        (attempt as u64) * WRITE_CONFLICT_BACKOFF_STEP_MS + jitter_ms,
    ));
}

pub const SECTIONS: &[&str] = &["input", "scripts", "outputs", "logs", "metadata"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchMcpOptions {
    pub root: String,
    pub max_bytes: usize,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkbenchToolError {
    code: &'static str,
    message: String,
    retryable: bool,
    details: Value,
}

impl WorkbenchToolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            code: "WorkbenchToolError",
            message: message.into(),
            retryable: false,
            details: json!({}),
        }
    }

    fn typed(
        code: &'static str,
        message: impl Into<String>,
        retryable: bool,
        details: Value,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details,
        }
    }

    pub fn as_value(&self) -> Value {
        json!({
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "details": self.details,
        })
    }
}

impl fmt::Display for WorkbenchToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorkbenchToolError {}

pub fn normalize_workbench_root(raw: &str) -> Result<String, String> {
    let normalized = normalize_absolute_path(raw, "workbench_root")?;
    if normalized == "/" {
        return Err("workbench_root must not be /".to_owned());
    }
    Ok(normalized)
}

pub fn tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "workbench_create",
            description:
                "Create a NoKV-controlled workbench directory with input, scripts, outputs, logs, and metadata sections.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string", "description": "Workbench id, e.g. spedas-task-001."}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_put_file",
            description:
                "Publish one file into a jailed workbench section. Paths are relative to the section; overwrite requires replace=true.",
            parameters: json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": SECTIONS},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "text": {"type": "string"},
                    "base64": {"type": "string"},
                    "content_type": {"type": "string"},
                    "replace": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_append",
            description:
                "Append bytes to the end of one workbench file, creating it when missing. Safe under concurrent writers: conflicting appends are retried automatically. After an append, stat digest_uri describes the appended delta bytes, not the full content sha256.",
            parameters: json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": SECTIONS},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "text": {"type": "string"},
                    "base64": {"type": "string"},
                    "content_type": {"type": "string"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_edit",
            description:
                "Replace an exact string in one workbench text file. Fails when old_string is missing or not unique unless replace_all=true; concurrent writes are retried with re-validation.",
            parameters: json!({
                "type": "object",
                "required": ["id", "section", "path", "old_string", "new_string"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": SECTIONS},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "old_string": {"type": "string"},
                    "new_string": {"type": "string"},
                    "replace_all": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_list",
            description:
                "List a workbench, section, or subdirectory through the NoKV namespace. Not recursive. Entries written outside the standard sections by other NoKV clients are returned with section null; such entries cannot be addressed by the other workbench tools. Pass at_snapshot (a snapshot id or a checkpoint name from workbench_snapshot) to list the subtree as it was at that checkpoint; an expired or reaped snapshot fails loudly.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_LIST_LIMIT},
                    "at_snapshot": at_snapshot_parameter_schema()
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_stat",
            description:
                "Inspect a workbench, section, subdirectory, or file compact card through the NoKV namespace. Pass at_snapshot (a snapshot id or a checkpoint name from workbench_snapshot) to stat the path as it was at that checkpoint; an expired or reaped snapshot fails loudly.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "at_snapshot": at_snapshot_parameter_schema()
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_read",
            description:
                "Read one workbench file through the NoKV namespace. Structured mode returns JSON, YAML, or text records; bytes mode returns byte ranges as a base64 string in bytes (bytes_encoding is \"base64\"). Pass if_none_match with a previously returned generation to skip the body when the file is unchanged. Pass at_snapshot (a snapshot id or a checkpoint name from workbench_snapshot) to read the file as it was at that checkpoint: bytes mode reads a byte range, any other mode returns text_lines for UTF-8 text (offset and limit count lines) and errors for non-text content (structured record reads at a snapshot are not yet supported); an expired or reaped snapshot fails loudly.",
            parameters: json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": SECTIONS},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0, "description": "Start offset for the read; ignored when cursor is set. Bytes are counted in bytes; a text at_snapshot read counts lines."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_READ_LIMIT},
                    "if_none_match": {"type": "integer", "minimum": 0, "description": "Generation from a previous response. When it still matches, the file body is skipped and the response carries not_modified=true plus the unchanged generation."},
                    "at_snapshot": at_snapshot_parameter_schema()
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_grep",
            description:
                "Search workbench file bodies for case-insensitive literal substrings. This is not regex grep. patterns adds OR alternatives (at most 16); when patterns is omitted and pattern contains '|', pattern is split on '|' into OR alternatives (empty segments dropped, same 16-alternative cap). glob filters file basenames with * and ?. Matches in files written outside the standard sections by other NoKV clients are returned with section null; such files cannot be addressed by the other workbench tools.",
            parameters: json!({
                "type": "object",
                "required": ["id", "pattern", "recursive"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "pattern": {"type": "string"},
                    "patterns": {"type": "array", "items": {"type": "string"}, "maxItems": MAX_WORKBENCH_GREP_PATTERNS},
                    "glob": {"type": ["string", "null"], "description": "Basename filter supporting * and ?."},
                    "recursive": {"type": "boolean"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_GREP_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_search",
            description:
                "Query workbench paths by metadata with catalog field predicates, sort, projections, and facets. Omit id to search across every workbench under the root. Complements workbench_find, which discovers workbenches by their committed manifest; use workbench_grep for file content search. Directories created under the root by other NoKV clients appear with their directory name as workbench_id; such entries cannot be addressed by the other workbench tools.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"], "description": "Workbench id. Omit to search across all workbenches."},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "predicates": predicates_parameter_schema(),
                    "sort": sort_parameter_schema(),
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "facets": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_SEARCH_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_aggregate",
            description:
                "Compute summary rows over workbench paths using catalog field ids: count, sum, avg, min, max, group, filter, and sort. Omit id to aggregate across every workbench under the root.",
            parameters: json!({
                "type": "object",
                "required": ["measures"],
                "properties": {
                    "id": {"type": ["string", "null"], "description": "Workbench id. Omit to aggregate across all workbenches."},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "predicates": predicates_parameter_schema(),
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
                    "sort": sort_parameter_schema(),
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_AGGREGATE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_catalog",
            description:
                "Discover catalog field ids for workbench_search and workbench_aggregate predicates, projections, sort, facets, and measures. Omit id to inspect the workbench root.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"], "description": "Workbench id. Omit to inspect the workbench root."},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "field_prefix": {"type": ["string", "null"]},
                    "include_facets": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_find",
            description:
                "List workbenches across the workbench root with optional committed-state and manifest substring filters. Returns compact manifest summaries by default.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "committed": {"type": ["boolean", "null"], "description": "Filter by completion marker. Null or omitted returns all workbenches."},
                    "manifest_pattern": {"type": ["string", "null"], "description": "Case-insensitive literal substring filter over metadata/run_manifest.json."},
                    "include_manifest": {"type": "boolean", "description": "Include full run_manifest.json envelopes. Defaults false."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_WORKBENCH_FIND_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_commit",
            description:
                "Mark a workbench complete by publishing metadata/run_manifest.json. This is the v0 commit point.",
            parameters: json!({
                "type": "object",
                "required": ["id", "manifest"],
                "properties": {
                    "id": {"type": "string"},
                    "manifest": {"type": "object"},
                    "replace": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_snapshot",
            description:
                "Snapshot a committed workbench subtree and hold it under a lease. Returns the NoKV snapshot id, read version, and lease_expires_at (unix ms). The lease defaults to 7 days (ttl_days), capped at 90; longer holds are not a lease knob (wait for named refs or use the CLI). Pass name ([A-Za-z0-9_-]{1,64}) to alias the checkpoint for later renew/list/at_snapshot reads. A lease expresses liveness, not archival importance: an unrenewed snapshot is reaped after it expires and the point-in-time view is lost (current files remain).",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": ["string", "null"], "description": "Checkpoint alias matching [A-Za-z0-9_-]{1,64}. Resolves to this snapshot in workbench_snapshot_renew, workbench_snapshot_list, and at_snapshot reads."},
                    "ttl_days": {"type": "integer", "minimum": 1, "maximum": MAX_SNAPSHOT_TTL_DAYS, "description": "Lease length in days. Defaults to 7; values above 90 are rejected."}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_snapshot_renew",
            description:
                "Extend the lease on a workbench snapshot before it is reaped. Identify it by snapshot_id or by the name given at mint time (resolved through the workbench checkpoint registry). ttl_days sets the new lease length from now (default 7, max 90). A snapshot already reaped after lease expiry cannot be renewed; re-mint from the current state instead.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "snapshot_id": {"type": ["integer", "null"], "minimum": 0, "description": "Snapshot id to renew. Provide exactly one of snapshot_id or name."},
                    "name": {"type": ["string", "null"], "description": "Checkpoint name to renew. Provide exactly one of snapshot_id or name."},
                    "ttl_days": {"type": "integer", "minimum": 1, "maximum": MAX_SNAPSHOT_TTL_DAYS, "description": "New lease length in days from now. Defaults to 7; values above 90 are rejected."}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_snapshot_list",
            description:
                "List a workbench's checkpoints from its registry, each joined with live pin state: alive, expired (reap pending), or reaped. Returns an empty list when the workbench has no registry yet. Use the snapshot ids or names here with workbench_snapshot_renew or the at_snapshot argument of workbench_stat, workbench_list, and workbench_read.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"}
                },
                "additionalProperties": false
            }),
        },
    ]
}

/// Shared schema for the `at_snapshot` argument: either a numeric snapshot id or
/// a checkpoint name string. `additionalProperties:false` schemas above embed
/// this so the two accepted shapes stay in sync across the read tools.
fn at_snapshot_parameter_schema() -> Value {
    json!({
        "anyOf": [
            {"type": "integer", "minimum": 0},
            {"type": "string"},
            {"type": "null"}
        ],
        "description": "Read at a checkpoint: a snapshot id (integer) or a checkpoint name (string) from workbench_snapshot."
    })
}

// Mirrors of the agent find/aggregate sub-schemas (nokv-agent
// agent_tool_definitions); keep them in sync when the agent schemas change.
fn predicates_parameter_schema() -> Value {
    json!({
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
    })
}

fn sort_parameter_schema() -> Value {
    json!({
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
    })
}

pub fn execute_tool<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    name: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    match name {
        "workbench_create" => create_workbench(client, options, args),
        "workbench_put_file" => put_file(client, options, args),
        "workbench_append" => append_file(client, options, args),
        "workbench_edit" => edit_file(client, options, args),
        "workbench_list" => execute_read_tool(client, options, "ls", args),
        "workbench_stat" => execute_read_tool(client, options, "stat", args),
        "workbench_read" => execute_read_tool(client, options, "read", args),
        "workbench_grep" => execute_read_tool(client, options, "grep", args),
        "workbench_search" => execute_query_tool(client, options, "find", args),
        "workbench_aggregate" => execute_query_tool(client, options, "aggregate", args),
        "workbench_catalog" => execute_query_tool(client, options, "catalog", args),
        "workbench_find" => find_workbenches(client, options, args),
        "workbench_commit" => commit_workbench(client, options, args),
        "workbench_snapshot" => snapshot_workbench(client, options, args),
        "workbench_snapshot_renew" => renew_snapshot_workbench(client, options, args),
        "workbench_snapshot_list" => list_snapshots_workbench(client, options, args),
        other => Err(WorkbenchToolError::new(format!(
            "unknown workbench tool {other}"
        ))),
    }
}

fn create_workbench<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    ensure_standard_dirs(client, options, &id)?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": workbench_path(options, &id),
        "sections": SECTIONS,
    }))
}

fn put_file<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let section = required_section(args)?;
    let rel_path = required_section_relative_path(args, section, "path")?;
    let replace = optional_bool(args, "replace")?.unwrap_or(false);
    let (bytes, default_content_type) = payload_bytes(args, options.max_bytes)?;
    let content_type = optional_string(args, "content_type")?
        .unwrap_or(default_content_type)
        .to_owned();

    ensure_standard_dirs(client, options, &id)?;
    ensure_parent_dirs(client, options, &id, section, &rel_path)?;
    let path = section_path(options, &id, section, Some(&rel_path));
    let digest_uri = digest_uri(&bytes);
    let metadata = artifact_metadata(options, &path, &digest_uri, &content_type);
    let entry = if replace {
        client
            .put_artifact_replace(&path, bytes, metadata)
            .map_err(client_error)?
            .entry
    } else {
        client
            .put_artifact(&path, bytes, metadata)
            .map_err(client_error)?
    };

    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "section": section,
        "relative_path": rel_path,
        "path": path,
        "size_bytes": entry.attr.size,
        "inode": entry.attr.inode.get(),
        "generation": entry.attr.generation,
        "digest_uri": digest_uri,
        "content_type": content_type,
        "replace": replace,
    }))
}

fn append_file<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let section = required_section(args)?;
    let rel_path = required_section_relative_path(args, section, "path")?;
    let (bytes, default_content_type) = payload_bytes(args, options.max_bytes)?;
    let content_type = optional_string(args, "content_type")?
        .unwrap_or(default_content_type)
        .to_owned();

    ensure_standard_dirs(client, options, &id)?;
    ensure_parent_dirs(client, options, &id, section, &rel_path)?;
    let path = section_path(options, &id, section, Some(&rel_path));
    // The digest covers the appended delta only: computing a full-content
    // sha256 would force a read of the whole file on every append.
    let digest_uri = digest_uri(&bytes);
    let appended_bytes = bytes.len();
    let mut attempts = 0;
    let outcome = loop {
        let metadata = artifact_metadata(options, &path, &digest_uri, &content_type);
        // append_artifact re-reads the current end offset on every call, so a
        // lost race against a concurrent writer is safe to retry as-is.
        match client.append_artifact(&path, bytes.clone(), metadata, None) {
            Ok(outcome) => break outcome,
            Err(err) if is_artifact_write_conflict(&err) && attempts < WRITE_CONFLICT_RETRIES => {
                attempts += 1;
                write_conflict_backoff(attempts);
            }
            Err(err) if is_artifact_write_conflict(&err) => {
                return Err(write_conflict_exhausted(attempts + 1, err))
            }
            Err(err) => return Err(client_error(err)),
        }
    };

    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "section": section,
        "relative_path": rel_path,
        "path": path,
        "appended_bytes": appended_bytes,
        "size_bytes": outcome.new_size,
        "generation": outcome.generation,
        "created": outcome.created,
    }))
}

fn edit_file<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let section = required_section(args)?;
    let rel_path = required_section_relative_path(args, section, "path")?;
    let old_string = required_string(args, "old_string")?;
    if old_string.is_empty() {
        return Err(WorkbenchToolError::new("old_string must not be empty"));
    }
    let new_string = required_string(args, "new_string")?;
    let replace_all = optional_bool(args, "replace_all")?.unwrap_or(false);
    let path = section_path(options, &id, section, Some(&rel_path));

    let mut attempts = 0;
    let (result, replacements) = loop {
        let entry = client
            .metadata()
            .lookup(&path)
            .map_err(client_error)?
            .ok_or_else(|| WorkbenchToolError::new(format!("path not found: {path}")))?;
        if entry.attr.file_type != FileType::File {
            return Err(WorkbenchToolError::new(format!(
                "path exists but is not a file: {path}"
            )));
        }
        if entry.attr.size > options.max_bytes as u64 {
            return Err(WorkbenchToolError::new(format!(
                "file exceeds max_bytes: {} > {}",
                entry.attr.size, options.max_bytes
            )));
        }
        let text = String::from_utf8(client.cat(&path).map_err(client_error)?)
            .map_err(|err| WorkbenchToolError::new(format!("file is not valid UTF-8: {err}")))?;
        let count = text.matches(old_string).count();
        if count == 0 {
            return Err(WorkbenchToolError::new(format!(
                "old_string not found in {path}"
            )));
        }
        if count > 1 && !replace_all {
            return Err(WorkbenchToolError::new(format!(
                "old_string found {count} times — use replace_all=true or provide more context"
            )));
        }
        let replacements = if replace_all { count } else { 1 };
        let new_text = if replace_all {
            text.replace(old_string, new_string)
        } else {
            text.replacen(old_string, new_string, 1)
        };
        if new_text == text {
            // A byte-identical replacement publishing a new generation would
            // invalidate if_none_match caches and CAS pins for nothing;
            // report success at the current state instead.
            return Ok(json!({
                "status": "success",
                "workbench_id": id,
                "section": section,
                "relative_path": rel_path,
                "path": path,
                "replacements": replacements,
                "size_bytes": entry.attr.size,
                "generation": entry.attr.generation,
                "no_change": true,
            }));
        }
        let new_bytes = new_text.into_bytes();
        if new_bytes.len() > options.max_bytes {
            return Err(WorkbenchToolError::new(format!(
                "payload exceeds max_bytes: {} > {}",
                new_bytes.len(),
                options.max_bytes
            )));
        }
        let body = entry
            .body
            .as_ref()
            .ok_or_else(|| WorkbenchToolError::new(format!("path has no file body: {path}")))?;
        let digest_uri = digest_uri(&new_bytes);
        let metadata = artifact_metadata(options, &path, &digest_uri, &body.content_type);
        match client.put_artifact_replace_if_generation(&path, new_bytes, metadata, body.generation)
        {
            Ok(result) => break (result, replacements),
            Err(err) if is_artifact_write_conflict(&err) && attempts < WRITE_CONFLICT_RETRIES => {
                // A writer landed since our read; re-read and re-validate.
                attempts += 1;
                write_conflict_backoff(attempts);
            }
            Err(err) if is_artifact_write_conflict(&err) => {
                return Err(write_conflict_exhausted(attempts + 1, err))
            }
            Err(err) => return Err(client_error(err)),
        }
    };

    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "section": section,
        "relative_path": rel_path,
        "path": path,
        "replacements": replacements,
        "size_bytes": result.entry.attr.size,
        "generation": result.entry.attr.generation,
        "no_change": false,
    }))
}

fn execute_read_tool<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    read_tool: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let target = match read_tool {
        "read" => {
            let section = required_section(args)?;
            let rel_path = required_section_relative_path(args, section, "path")?;
            section_path(options, &id, section, Some(&rel_path))
        }
        _ => scoped_path_from_optional_args(options, &id, args)?,
    };
    if let Some(at_snapshot) = args.get("at_snapshot").filter(|value| !value.is_null()) {
        return execute_at_snapshot_read_tool(
            client,
            options,
            &id,
            read_tool,
            &target,
            at_snapshot,
            args,
        );
    }
    if read_tool == "read" {
        if let Some(expected) = optional_u64(args, "if_none_match")? {
            // A missing ancestor and a missing leaf are the same absence to
            // the caller; fold both into one not-found message.
            let metadata = stat_path_or_absent(client, &target)?
                .ok_or_else(|| WorkbenchToolError::new(format!("path not found: {target}")))?;
            // Only a file body can be conditionally skipped. Anything else
            // falls through to the main read path so a directory fails with
            // the same error an unconditional read reports.
            if metadata.attr.file_type == FileType::File && metadata.attr.generation == expected {
                let scope = path_scope(options, &id, &target)?;
                return Ok(json!({
                    "status": "success",
                    "workbench_id": id,
                    "workbench_path": workbench_path(options, &id),
                    "section": scope.section,
                    "relative_path": scope.relative_path,
                    "path": scope.path,
                    "not_modified": true,
                    "generation": expected,
                }));
            }
        }
    }
    let mut forwarded = args
        .as_object()
        .cloned()
        .ok_or_else(|| WorkbenchToolError::new("tool arguments must be a JSON object"))?;
    forwarded.insert("path".to_owned(), Value::String(target.clone()));
    forwarded.remove("id");
    forwarded.remove("section");
    match read_tool {
        "ls" | "stat" => {
            forwarded.remove("format");
            forwarded.remove("offset");
            forwarded.remove("pattern");
            forwarded.remove("recursive");
        }
        "read" => {
            forwarded.remove("pattern");
            forwarded.remove("recursive");
            forwarded.remove("if_none_match");
        }
        "grep" => {
            forwarded.remove("format");
            forwarded.remove("offset");
            split_piped_grep_pattern(&mut forwarded)?;
        }
        _ => {}
    }
    let result = nokv_agent::execute_agent_tool(client, read_tool, &Value::Object(forwarded))
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    shape_read_tool_result(client, options, &id, &target, read_tool, result)
}

/// `pattern: "a|b"` without an explicit `patterns` array is treated as OR
/// alternatives, matching what agents expect from grep pipes. Empty segments
/// are dropped; a pattern of only pipes stays a literal search. More
/// alternatives than the server accepts fail here with an actionable error
/// instead of the server's opaque `patterns` cap message.
fn split_piped_grep_pattern(
    forwarded: &mut serde_json::Map<String, Value>,
) -> Result<(), WorkbenchToolError> {
    let has_patterns = forwarded
        .get("patterns")
        .and_then(Value::as_array)
        .is_some_and(|patterns| !patterns.is_empty());
    if has_patterns {
        return Ok(());
    }
    let Some(pattern) = forwarded.get("pattern").and_then(Value::as_str) else {
        return Ok(());
    };
    if !pattern.contains('|') {
        return Ok(());
    }
    let alternatives = pattern
        .split('|')
        .filter(|part| !part.is_empty())
        .map(|part| Value::String(part.to_owned()))
        .collect::<Vec<_>>();
    if alternatives.len() > MAX_WORKBENCH_GREP_PATTERNS {
        return Err(WorkbenchToolError::new(format!(
            "pattern contains {} '|'-separated alternatives; at most {MAX_WORKBENCH_GREP_PATTERNS} are supported",
            alternatives.len()
        )));
    }
    if !alternatives.is_empty() {
        forwarded.insert("patterns".to_owned(), Value::Array(alternatives));
    }
    Ok(())
}

fn shape_read_tool_result<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    target: &str,
    read_tool: &str,
    result: Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let scope = path_scope(options, id, target)?;
    match read_tool {
        "ls" => shape_list_result(options, id, &scope, result),
        "stat" => shape_stat_result(client, options, id, &scope, result),
        "read" => shape_file_read_result(options, id, &scope, result),
        "grep" => shape_grep_result(options, id, &scope, result),
        other => Err(WorkbenchToolError::new(format!(
            "unsupported read tool {other}"
        ))),
    }
}

fn shape_list_result(
    options: &WorkbenchMcpOptions,
    id: &str,
    scope: &WorkbenchPathScope,
    result: Value,
) -> Result<Value, WorkbenchToolError> {
    let entries = result
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| WorkbenchToolError::new("ls result missing entries"))?
        .iter()
        .map(|entry| compact_list_entry(options, id, entry))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "entry_count": result.get("entry_count").cloned().unwrap_or(Value::Null),
        "entries": entries,
        "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

fn shape_stat_result<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    scope: &WorkbenchPathScope,
    result: Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let card = result
        .get("card")
        .ok_or_else(|| WorkbenchToolError::new("stat result missing card"))?;
    let metadata = client
        .metadata()
        .stat_path(&scope.path)
        .map_err(client_error)?
        .ok_or_else(|| WorkbenchToolError::new(format!("path not found: {}", scope.path)))?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "card": compact_stat_card(scope, card, &metadata),
    }))
}

fn shape_file_read_result(
    options: &WorkbenchMcpOptions,
    id: &str,
    scope: &WorkbenchPathScope,
    result: Value,
) -> Result<Value, WorkbenchToolError> {
    // A bytes-mode page arrives as a JSON integer array; re-encode it as
    // base64 so each output byte costs ~1.3 characters instead of the ~4
    // tokens an integer element burns. Structured mode has no bytes field.
    let (bytes, bytes_encoding) = match result.get("bytes") {
        Some(Value::Array(values)) => {
            let raw = values
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .and_then(|byte| u8::try_from(byte).ok())
                        .ok_or_else(|| {
                            WorkbenchToolError::new("read result bytes contain a non-byte value")
                        })
                })
                .collect::<Result<Vec<u8>, _>>()?;
            (
                Value::String(base64::engine::general_purpose::STANDARD.encode(raw)),
                json!("base64"),
            )
        }
        _ => (Value::Null, Value::Null),
    };
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "generation": result.get("generation").cloned().unwrap_or(Value::Null),
        "total_size_bytes": result.get("total_size_bytes").cloned().unwrap_or(Value::Null),
        "format": result.get("format").cloned().unwrap_or(Value::Null),
        "record_type": result.get("record_type").cloned().unwrap_or(Value::Null),
        "record_count": result.get("record_count").cloned().unwrap_or(Value::Null),
        "cursor": result.get("cursor").cloned().unwrap_or(Value::Null),
        "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
        "items": result.get("items").cloned().unwrap_or_else(|| json!([])),
        "bytes": bytes,
        "bytes_encoding": bytes_encoding,
    }))
}

fn shape_grep_result(
    options: &WorkbenchMcpOptions,
    id: &str,
    scope: &WorkbenchPathScope,
    result: Value,
) -> Result<Value, WorkbenchToolError> {
    let matches = result
        .get("matches")
        .and_then(Value::as_array)
        .ok_or_else(|| WorkbenchToolError::new("grep result missing matches"))?
        .iter()
        .map(|match_| compact_grep_match(options, id, match_))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "pattern": result.get("pattern").cloned().unwrap_or(Value::Null),
        "recursive": result.get("recursive").cloned().unwrap_or(Value::Bool(false)),
        "matches": matches,
        "files_scanned": result.get("files_scanned").cloned().unwrap_or(Value::Null),
        "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

/// Thin wrapper over the agent query tools (`find`, `aggregate`, `catalog`):
/// translates workbench scoping into an absolute path, forwards the remaining
/// arguments untouched, and enriches find matches with workbench coordinates.
fn execute_query_tool<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    query_tool: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = match optional_string(args, "id")? {
        Some(id) => {
            validate_workbench_id(id)?;
            Some(id.to_owned())
        }
        None => None,
    };
    let target = match &id {
        Some(id) => scoped_path_from_optional_args(options, id, args)?,
        None => {
            if optional_string(args, "section")?.is_some() {
                return Err(WorkbenchToolError::new(
                    "section requires id when querying across the workbench root",
                ));
            }
            if optional_string(args, "path")?.is_some() {
                return Err(WorkbenchToolError::new(
                    "path requires id when querying across the workbench root",
                ));
            }
            options.root.clone()
        }
    };
    // A root that no workbench has materialized yet is an empty result for
    // cross-workbench queries, not an error.
    if id.is_none()
        && matches!(query_tool, "find" | "aggregate" | "catalog")
        && stat_path_or_absent(client, &options.root)?.is_none()
    {
        return Ok(empty_query_result(query_tool, &options.root));
    }
    let mut forwarded = args
        .as_object()
        .cloned()
        .ok_or_else(|| WorkbenchToolError::new("tool arguments must be a JSON object"))?;
    forwarded.insert("path".to_owned(), Value::String(target.clone()));
    forwarded.remove("id");
    forwarded.remove("section");
    let result = nokv_agent::execute_agent_tool(client, query_tool, &Value::Object(forwarded))
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    match query_tool {
        "find" => shape_search_result(options, &target, result),
        "aggregate" | "catalog" => {
            let mut object = result
                .as_object()
                .cloned()
                .ok_or_else(|| WorkbenchToolError::new(format!("{query_tool} result malformed")))?;
            object.insert("status".to_owned(), json!("success"));
            Ok(Value::Object(object))
        }
        other => Err(WorkbenchToolError::new(format!(
            "unsupported query tool {other}"
        ))),
    }
}

fn empty_query_result(query_tool: &str, root: &str) -> Value {
    match query_tool {
        "find" => json!({
            "status": "success",
            "path": root,
            "match_count": 0,
            "matches": [],
            "facets": [],
            "next_cursor": Value::Null,
            "truncated": false,
        }),
        // Field-level mirror of the agent catalog output (nokv-agent
        // execute_catalog) for a root with no fields to discover.
        "catalog" => json!({
            "status": "success",
            "path": root,
            "catalog_empty": true,
            "catalog": {
                "filterable": [],
                "sortable": [],
                "facetable": [],
                "facets": [],
            },
            "child_catalogs": [],
        }),
        _ => json!({
            "status": "success",
            "path": root,
            "input_match_count": 0,
            "row_count": 0,
            "group_count": 0,
            "groups": [],
            "truncated": false,
        }),
    }
}

fn shape_search_result(
    options: &WorkbenchMcpOptions,
    target: &str,
    result: Value,
) -> Result<Value, WorkbenchToolError> {
    let matches = result
        .get("matches")
        .and_then(Value::as_array)
        .ok_or_else(|| WorkbenchToolError::new("find result missing matches"))?
        .iter()
        .map(|match_| enrich_search_match(options, match_))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "status": "success",
        "path": target,
        "match_count": result.get("match_count").cloned().unwrap_or(Value::Null),
        "matches": matches,
        "facets": result.get("facets").cloned().unwrap_or_else(|| json!([])),
        "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

/// A match path is `<root>/<workbench_id>[/...]`: the first segment below the
/// root names the workbench and the rest is scoped like enumeration output.
fn enrich_search_match(
    options: &WorkbenchMcpOptions,
    match_: &Value,
) -> Result<Value, WorkbenchToolError> {
    let path = match_
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| WorkbenchToolError::new("find match missing path"))?;
    let prefix = format!("{}/", options.root);
    let Some(rest) = path.strip_prefix(&prefix) else {
        // The root itself can satisfy a predicate; it has no workbench
        // coordinates to enrich with.
        return Ok(json!({
            "workbench_id": Value::Null,
            "path": path,
            "section": Value::Null,
            "relative_path": Value::Null,
            "values": match_.get("values").cloned().unwrap_or(Value::Null),
        }));
    };
    let workbench_id = rest.split('/').next().unwrap_or(rest);
    let scope = enumerated_path_scope(options, workbench_id, path)?;
    Ok(json!({
        "workbench_id": workbench_id,
        "path": scope.path,
        "section": scope.section,
        "relative_path": scope.relative_path,
        "values": match_.get("values").cloned().unwrap_or(Value::Null),
    }))
}

fn find_workbenches<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let committed_filter = optional_bool(args, "committed")?;
    let manifest_pattern = optional_string(args, "manifest_pattern")?;
    let include_manifest = optional_bool(args, "include_manifest")?.unwrap_or(false);
    let cursor = optional_string(args, "cursor")?;
    let limit = optional_usize(args, "limit")?.unwrap_or(DEFAULT_WORKBENCH_FIND_LIMIT);
    if limit == 0 || limit > MAX_WORKBENCH_FIND_LIMIT {
        return Err(WorkbenchToolError::new(format!(
            "limit must be between 1 and {MAX_WORKBENCH_FIND_LIMIT}"
        )));
    }

    if stat_path_or_absent(client, &options.root)?.is_none() {
        return Ok(json!({
            "status": "success",
            "path": options.root.clone(),
            "matches": [],
            "match_count": 0,
            "entry_count": 0,
            "next_cursor": Value::Null,
            "truncated": false,
        }));
    }

    let list_args = json!({
        "path": options.root,
        "cursor": cursor,
        "limit": limit,
    });
    let page = nokv_agent::execute_agent_tool(client, "ls", &list_args)
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    let entries = page
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| WorkbenchToolError::new("workbench root listing missing entries"))?;
    let mut matches = Vec::new();
    for entry in entries {
        if entry.get("kind").and_then(Value::as_str) != Some("directory") {
            continue;
        }
        let Some(id) = entry.get("name").and_then(Value::as_str) else {
            continue;
        };
        let summary = workbench_manifest_summary(client, options, id, include_manifest)?;
        if let Some(committed) = committed_filter {
            if summary.committed != committed {
                continue;
            }
        }
        if let Some(pattern) = manifest_pattern {
            if !summary.matches_manifest_pattern(pattern) {
                continue;
            }
        }
        matches.push(summary.into_json(options, id));
    }

    let match_count = matches.len();
    Ok(json!({
        "status": "success",
        "path": options.root.clone(),
        "matches": matches,
        "match_count": match_count,
        "entry_count": page.get("entry_count").cloned().unwrap_or(Value::Null),
        "next_cursor": page.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": page.get("truncated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

fn commit_workbench<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let manifest = args
        .get("manifest")
        .cloned()
        .ok_or_else(|| WorkbenchToolError::new("missing required argument manifest"))?;
    if !manifest.is_object() {
        return Err(WorkbenchToolError::new("manifest must be a JSON object"));
    }
    let replace = optional_bool(args, "replace")?.unwrap_or(false);
    ensure_standard_dirs(client, options, &id)?;
    let path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    let envelope = json!({
        "schema": "nokv.workbench.run_manifest.v0",
        "workbench_id": id,
        "workbench_path": workbench_path(options, &id),
        "committed_at_unix_seconds": unix_seconds(),
        "manifest": manifest,
    });
    let bytes = serde_json::to_vec_pretty(&envelope)
        .map_err(|err| WorkbenchToolError::new(format!("failed to encode manifest: {err}")))?;
    let digest_uri = digest_uri(&bytes);
    let metadata = artifact_metadata(options, &path, &digest_uri, "application/json");
    let entry = if replace {
        client
            .put_artifact_replace(&path, bytes, metadata)
            .map_err(client_error)?
            .entry
    } else {
        client
            .put_artifact(&path, bytes, metadata)
            .map_err(client_error)?
    };
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": path,
        "size_bytes": entry.attr.size,
        "inode": entry.attr.inode.get(),
        "generation": entry.attr.generation,
        "digest_uri": digest_uri,
        "replace": replace,
    }))
}

fn snapshot_workbench<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let name = match optional_string(args, "name")? {
        Some(raw) => {
            validate_snapshot_name(raw)?;
            Some(raw.to_owned())
        }
        None => None,
    };
    let (ttl_days, ttl_defaulted) = resolve_ttl_days(args)?;
    let manifest_path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    if stat_path_or_absent(client, &manifest_path)?.is_none() {
        return Err(WorkbenchToolError::new(format!(
            "workbench {id} is not committed; missing {manifest_path}"
        )));
    }
    let path = workbench_path(options, &id);
    let lease_ms = ttl_days.saturating_mul(MS_PER_DAY);
    // The requested lease is part of the mint RPC, so the returned pin is the
    // authoritative checkpoint and creation costs one metadata round trip.
    let snapshot = client
        .metadata()
        .snapshot_subtree_path_with_lease(&path, lease_ms)
        .map_err(client_error)?;
    let snapshot_id = snapshot.snapshot_id;
    let lease_expires_unix_ms = Some(snapshot.lease_expires_unix_ms);
    let read_version = snapshot.read_version;
    let created_at = unix_ms();
    let registry_entry = json!({
        "name": name,
        "snapshot_id": snapshot_id,
        "read_version": read_version,
        "lease_expires_unix_ms": lease_expires_unix_ms,
        "created_at": created_at,
        "reason": "mint",
    });
    let registry = registry_write_status(append_checkpoint_registry_line(
        client,
        options,
        &id,
        &registry_entry,
    ));
    let mut out = json!({
        "status": "success",
        "workbench_id": id,
        "path": path,
        "snapshot_id": snapshot_id,
        "read_version": read_version,
        "name": name,
        "ttl_days": ttl_days,
        "lease_expires_at": lease_expires_unix_ms,
        "lease_expires_unix_ms": lease_expires_unix_ms,
        "registry": registry,
    });
    if ttl_defaulted {
        out["expiry_warning"] = json!(format!(
            "lease defaulted to {DEFAULT_SNAPSHOT_TTL_DAYS} days; this snapshot is reaped after it expires unless renewed. Renew before a handoff that must outlive the lease, or pass ttl_days."
        ));
    }
    Ok(out)
}

fn renew_snapshot_workbench<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let (ttl_days, _ttl_defaulted) = resolve_ttl_days(args)?;
    let snapshot_id = resolve_renew_target(client, options, &id, args)?;
    let lease_ms = ttl_days.saturating_mul(MS_PER_DAY);
    let path = workbench_path(options, &id);
    let outcome = client
        .metadata()
        .renew_snapshot(&path, snapshot_id, lease_ms)
        .map_err(client_error)?;
    let (pin, extended) = match outcome {
        nokv_meta::SnapshotRenewOutcome::Renewed { pin, extended } => (pin, extended),
        nokv_meta::SnapshotRenewOutcome::Missing { .. } => {
            return Err(WorkbenchToolError::typed(
                "SnapshotNotFound",
                format!(
                    "snapshot {snapshot_id} was reaped after lease expiry; re-mint from current state"
                ),
                false,
                json!({"snapshot_id": snapshot_id}),
            ));
        }
    };
    let lease_expires_unix_ms = Some(pin.lease_expires_unix_ms);
    let read_version = Some(pin.read_version);
    // Carry the name forward from the mint record so the renew row stays joinable
    // with the checkpoint it extends.
    let name = registry_name_for_snapshot(client, options, &id, snapshot_id)?;
    let created_at = unix_ms();
    let registry_entry = json!({
        "name": name,
        "snapshot_id": snapshot_id,
        "read_version": read_version,
        "lease_expires_unix_ms": lease_expires_unix_ms,
        "created_at": created_at,
        "reason": "renew",
    });
    let registry = registry_write_status(append_checkpoint_registry_line(
        client,
        options,
        &id,
        &registry_entry,
    ));
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "snapshot_id": snapshot_id,
        "name": name,
        "renewed": true,
        "extended": extended,
        "ttl_days": ttl_days,
        "read_version": read_version,
        "lease_expires_at": lease_expires_unix_ms,
        "lease_expires_unix_ms": lease_expires_unix_ms,
        "registry": registry,
    }))
}

#[derive(Default)]
struct CheckpointGroup {
    name: Value,
    created_at: Value,
    read_version: Value,
    has_mint: bool,
    registered_lease: Value,
    renew_count: u64,
    last_renewed_at: Value,
}

fn list_snapshots_workbench<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let id = required_workbench_id(args)?;
    let entries = read_checkpoint_registry(client, options, &id)?;
    let root_path = workbench_path(options, &id);
    let mut order = Vec::new();
    let mut groups: std::collections::HashMap<u64, CheckpointGroup> =
        std::collections::HashMap::new();
    for entry in &entries {
        let Some(snapshot_id) = entry.get("snapshot_id").and_then(Value::as_u64) else {
            continue;
        };
        let group = groups.entry(snapshot_id).or_insert_with(|| {
            order.push(snapshot_id);
            CheckpointGroup::default()
        });
        if let Some(lease) = entry.get("lease_expires_unix_ms") {
            group.registered_lease = lease.clone();
        }
        if entry.get("reason").and_then(Value::as_str) == Some("renew") {
            group.renew_count += 1;
            group.last_renewed_at = entry.get("created_at").cloned().unwrap_or(Value::Null);
        } else {
            group.has_mint = true;
            group.name = entry.get("name").cloned().unwrap_or(Value::Null);
            group.created_at = entry.get("created_at").cloned().unwrap_or(Value::Null);
            group.read_version = entry.get("read_version").cloned().unwrap_or(Value::Null);
        }
        if group.read_version.is_null() {
            group.read_version = entry.get("read_version").cloned().unwrap_or(Value::Null);
        }
    }

    let mut checkpoints = Vec::with_capacity(order.len());
    for snapshot_id in order {
        let group = &groups[&snapshot_id];
        let status = client
            .metadata()
            .snapshot_pin_status(&root_path, snapshot_id)
            .map_err(client_error)?;
        let (state, live_lease) = match status.pin {
            None => ("reaped", None),
            Some(pin) if status.server_now_ms >= pin.lease_expires_unix_ms => {
                ("expired", Some(pin.lease_expires_unix_ms))
            }
            Some(pin) => ("alive", Some(pin.lease_expires_unix_ms)),
        };
        checkpoints.push(json!({
            "name": group.name.clone(),
            "snapshot_id": snapshot_id,
            "read_version": group.read_version.clone(),
            "reason": if group.has_mint { "mint" } else { "renew" },
            "created_at": group.created_at.clone(),
            "renew_count": group.renew_count,
            "last_renewed_at": group.last_renewed_at.clone(),
            "registered_lease_expires_unix_ms": group.registered_lease.clone(),
            "live_lease_expires_unix_ms": live_lease,
            "lease_expires_at": live_lease,
            "server_now_ms": status.server_now_ms,
            "state": state,
        }));
    }
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "checkpoint_count": checkpoints.len(),
        "checkpoints": checkpoints,
    }))
}

/// At-snapshot read/stat/list. The lease is checked *before* any bytes are read
/// so a caller never silently observes a half-dead snapshot (unchanged files
/// still resolving while overwritten ones vanish); expiry is a loud error.
fn execute_at_snapshot_read_tool<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    read_tool: &str,
    target: &str,
    at_snapshot: &Value,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    if read_tool == "grep" {
        return Err(WorkbenchToolError::new(
            "workbench_grep does not support at_snapshot; use workbench_read or workbench_list at the snapshot",
        ));
    }
    let snapshot_id = resolve_at_snapshot(client, options, id, at_snapshot)?;
    let scope = path_scope(options, id, target)?;
    // Subtree-snapshot reads address paths relative to the snapshot root (the
    // workbench directory), so strip the workbench prefix; the absolute `target`
    // is kept only for shaping the response coordinates.
    let snap_path = snapshot_relative_path(options, id, target)?;
    match read_tool {
        "stat" => at_snapshot_stat(client, options, id, snapshot_id, &scope, &snap_path),
        "ls" => at_snapshot_list(client, options, id, snapshot_id, &scope, target, args),
        "read" => at_snapshot_read(client, options, id, snapshot_id, &scope, &snap_path, args),
        other => Err(WorkbenchToolError::new(format!(
            "at_snapshot is not supported for {other}"
        ))),
    }
}

/// Convert an absolute workbench path into a path relative to the workbench's
/// snapshot subtree root (the form the at-snapshot service calls expect):
/// the workbench root becomes `/`, and `<root>/outputs/x` becomes `/outputs/x`.
fn snapshot_relative_path(
    options: &WorkbenchMcpOptions,
    id: &str,
    target: &str,
) -> Result<String, WorkbenchToolError> {
    let base = workbench_path(options, id);
    if target == base {
        return Ok("/".to_owned());
    }
    let prefix = format!("{base}/");
    let rest = target.strip_prefix(&prefix).ok_or_else(|| {
        WorkbenchToolError::new(format!("path {target} is outside workbench {base}"))
    })?;
    Ok(format!("/{rest}"))
}

fn at_snapshot_stat<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    snapshot_id: u64,
    scope: &WorkbenchPathScope,
    snap_path: &str,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let metadata = client
        .metadata()
        .stat_path_at_snapshot(&workbench_path(options, id), snapshot_id, snap_path)
        .map_err(|err| snapshot_client_error(snapshot_id, err))?
        .ok_or_else(|| {
            WorkbenchToolError::new(format!(
                "path not found at snapshot {snapshot_id}: {}",
                scope.path
            ))
        })?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "at_snapshot": snapshot_id,
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "card": snapshot_stat_card(scope, &metadata),
    }))
}

fn at_snapshot_list<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    snapshot_id: u64,
    scope: &WorkbenchPathScope,
    target: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let limit = optional_usize(args, "limit")?.unwrap_or(MAX_WORKBENCH_LIST_LIMIT);
    if limit == 0 || limit > MAX_WORKBENCH_LIST_LIMIT {
        return Err(WorkbenchToolError::new(format!(
            "limit must be between 1 and {MAX_WORKBENCH_LIST_LIMIT}"
        )));
    }
    let cursor = optional_string(args, "cursor")?;
    let after = cursor
        .map(decode_name_cursor)
        .transpose()
        .map_err(|err| WorkbenchToolError::new(format!("invalid snapshot list cursor: {err}")))?;
    let snap_path = snapshot_relative_path(options, id, target)?;
    let page = client
        .metadata()
        .list_path_at_snapshot_page(
            &workbench_path(options, id),
            snapshot_id,
            &snap_path,
            after.as_ref(),
            limit,
        )
        .map_err(|err| snapshot_client_error(snapshot_id, err))?;
    let next_cursor = page.next_cursor.as_ref().map(encode_name_cursor);
    let truncated = next_cursor.is_some();
    let dentries = page.entries;
    let base = target.trim_end_matches('/');
    let mut entries = Vec::with_capacity(dentries.len());
    for dentry in &dentries {
        let name = String::from_utf8_lossy(dentry.dentry.name.as_bytes()).into_owned();
        let child_path = format!("{base}/{name}");
        let child_scope = enumerated_path_scope(options, id, &child_path)?;
        let is_file = dentry.attr.file_type == FileType::File;
        entries.push(json!({
            "name": name,
            "path": child_scope.path,
            "section": child_scope.section,
            "relative_path": child_scope.relative_path,
            "kind": file_type_kind(dentry.attr.file_type),
            "size_bytes": if is_file { json!(dentry.attr.size) } else { Value::Null },
            "entry_count": Value::Null,
        }));
    }
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "at_snapshot": snapshot_id,
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "entry_count": entries.len(),
        "entries": entries,
        "next_cursor": next_cursor,
        "truncated": truncated,
    }))
}

fn at_snapshot_read<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    snapshot_id: u64,
    scope: &WorkbenchPathScope,
    snap_path: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let metadata = client
        .metadata()
        .stat_path_at_snapshot(&workbench_path(options, id), snapshot_id, snap_path)
        .map_err(|err| snapshot_client_error(snapshot_id, err))?
        .ok_or_else(|| {
            WorkbenchToolError::new(format!(
                "path not found at snapshot {snapshot_id}: {}",
                scope.path
            ))
        })?;
    if metadata.attr.file_type != FileType::File {
        return Err(WorkbenchToolError::new(format!(
            "path is not a file at snapshot {snapshot_id}: {}",
            scope.path
        )));
    }
    let size = metadata.attr.size;
    let generation = metadata.attr.generation;
    let offset = optional_u64(args, "offset")?.unwrap_or(0);
    let limit = optional_usize(args, "limit")?;
    let bytes_mode = optional_string(args, "format")? == Some("bytes");
    let common = json!({
        "status": "success",
        "workbench_id": id,
        "workbench_path": workbench_path(options, id),
        "at_snapshot": snapshot_id,
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "path": scope.path.clone(),
        "generation": generation,
        "total_size_bytes": size,
    });
    let mut out = common
        .as_object()
        .cloned()
        .expect("common read envelope is an object");
    if bytes_mode {
        // Byte-range read: offset and limit count bytes. The max_bytes guard
        // bounds a single page directly at the source read.
        let remaining = size.saturating_sub(offset);
        let mut len = limit
            .map(|limit| limit as u64)
            .unwrap_or(remaining)
            .min(remaining);
        if len > options.max_bytes as u64 {
            len = options.max_bytes as u64;
        }
        let raw = client
            .read_snapshot(
                &workbench_path(options, id),
                snapshot_id,
                snap_path,
                offset,
                len as usize,
            )
            .map_err(|err| snapshot_client_error(snapshot_id, err))?;
        let returned = raw.len() as u64;
        out.insert("format".to_owned(), json!("bytes"));
        out.insert("record_type".to_owned(), Value::Null);
        out.insert("record_count".to_owned(), Value::Null);
        out.insert("cursor".to_owned(), Value::Null);
        out.insert("next_cursor".to_owned(), Value::Null);
        out.insert(
            "truncated".to_owned(),
            json!(offset.saturating_add(returned) < size),
        );
        out.insert("items".to_owned(), json!([]));
        out.insert(
            "bytes".to_owned(),
            json!(base64::engine::general_purpose::STANDARD.encode(&raw)),
        );
        out.insert("bytes_encoding".to_owned(), json!("base64"));
        return Ok(Value::Object(out));
    }
    // Text-lines shaping: whole file (bounded by max_bytes), offset and limit
    // count lines. Structured record reads at a snapshot are Phase 2.
    if size > options.max_bytes as u64 {
        return Err(WorkbenchToolError::new(format!(
            "file exceeds max_bytes at snapshot {snapshot_id}: {size} > {}; read it in bytes mode with offset and limit",
            options.max_bytes
        )));
    }
    let raw = client
        .read_snapshot(
            &workbench_path(options, id),
            snapshot_id,
            snap_path,
            0,
            size as usize,
        )
        .map_err(|err| snapshot_client_error(snapshot_id, err))?;
    let text = String::from_utf8(raw).map_err(|_| {
        WorkbenchToolError::new(format!(
            "at_snapshot read of {} is not UTF-8 text; structured record reads at a snapshot are not yet supported, use format=bytes",
            scope.path
        ))
    })?;
    let lines: Vec<&str> = text.lines().collect();
    let total_lines = lines.len();
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(total_lines);
    let end = match limit {
        Some(limit) => start.saturating_add(limit).min(total_lines),
        None => total_lines,
    };
    let items = lines[start..end]
        .iter()
        .enumerate()
        .map(|(offset_in_page, line)| {
            json!({
                "index": start + offset_in_page,
                "value": {"text": line},
            })
        })
        .collect::<Vec<_>>();
    out.insert("format".to_owned(), json!("structured"));
    out.insert("record_type".to_owned(), json!("text_lines"));
    out.insert("record_count".to_owned(), json!(total_lines));
    out.insert("cursor".to_owned(), Value::Null);
    out.insert("next_cursor".to_owned(), Value::Null);
    out.insert("truncated".to_owned(), json!(end < total_lines));
    out.insert("items".to_owned(), json!(items));
    out.insert("bytes".to_owned(), Value::Null);
    out.insert("bytes_encoding".to_owned(), Value::Null);
    Ok(Value::Object(out))
}

fn file_type_kind(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
        _ => "other",
    }
}

/// Compact stat card built from at-snapshot metadata alone (no agent card is
/// available at a historical version). Directory `entry_count` is left null:
/// counting children at a snapshot would need a second listing round trip.
fn snapshot_stat_card(scope: &WorkbenchPathScope, metadata: &PathMetadata) -> Value {
    let body = metadata.body.as_ref();
    let is_file = metadata.attr.file_type == FileType::File;
    let name = scope
        .path
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .map(|name| name.to_owned());
    json!({
        "name": name,
        "path": scope.path.clone(),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "kind": file_type_kind(metadata.attr.file_type),
        "size_bytes": if is_file { json!(metadata.attr.size) } else { Value::Null },
        "entry_count": Value::Null,
        "record_count": Value::Null,
        "inode": metadata.attr.inode.get(),
        "generation": metadata.attr.generation,
        "content_type": body.map(|body| body.content_type.clone()),
        "digest_uri": body.map(|body| body.digest_uri.clone()),
        "producer": body.map(|body| body.producer.clone()),
        "manifest_id": body.map(|body| body.manifest_id.clone()),
    })
}

fn validate_snapshot_name(name: &str) -> Result<(), WorkbenchToolError> {
    if name.is_empty() || name.len() > 64 {
        return Err(WorkbenchToolError::new(
            "name must be 1 to 64 characters matching [A-Za-z0-9_-]",
        ));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(WorkbenchToolError::new(
            "name may contain only ASCII letters, digits, '_' and '-'",
        ));
    }
    Ok(())
}

/// Resolve the requested `ttl_days`, returning the value and whether it was
/// defaulted (which drives the expiry warning). Rejects values above the cap
/// with guidance toward durable retention instead of a longer lease.
fn resolve_ttl_days(args: &Value) -> Result<(u64, bool), WorkbenchToolError> {
    match optional_u64(args, "ttl_days")? {
        None => Ok((DEFAULT_SNAPSHOT_TTL_DAYS, true)),
        Some(0) => Err(WorkbenchToolError::new("ttl_days must be at least 1")),
        Some(days) if days > MAX_SNAPSHOT_TTL_DAYS => Err(WorkbenchToolError::new(format!(
            "ttl_days {days} exceeds the maximum of {MAX_SNAPSHOT_TTL_DAYS} days; a lease is not durable retention. Wait for named refs (Phase 2) or hold it with the CLI (renew-snapshot) for longer."
        ))),
        Some(days) => Ok((days, false)),
    }
}

fn checkpoint_registry_path(options: &WorkbenchMcpOptions, id: &str) -> String {
    section_path(options, id, "metadata", Some(CHECKPOINT_REGISTRY_RELPATH))
}

/// Append one JSON line to the workbench checkpoint registry (dogfooding the
/// append path). A lost CAS against a concurrent mint is retried the same way
/// `workbench_append` retries, because `append_artifact` re-reads the end offset
/// on every attempt.
fn append_checkpoint_registry_line<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    entry: &Value,
) -> Result<(), WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let path = checkpoint_registry_path(options, id);
    let mut line = serde_json::to_vec(entry).map_err(|err| {
        WorkbenchToolError::new(format!("failed to encode checkpoint registry entry: {err}"))
    })?;
    line.push(b'\n');
    let digest_uri = digest_uri(&line);
    let mut attempts = 0;
    loop {
        let metadata = artifact_metadata(options, &path, &digest_uri, "application/x-ndjson");
        match client.append_artifact(&path, line.clone(), metadata, None) {
            Ok(_) => return Ok(()),
            Err(err) if is_artifact_write_conflict(&err) && attempts < WRITE_CONFLICT_RETRIES => {
                attempts += 1;
                write_conflict_backoff(attempts);
            }
            Err(err) if is_artifact_write_conflict(&err) => {
                return Err(write_conflict_exhausted(attempts + 1, err))
            }
            Err(err) => return Err(client_error(err)),
        }
    }
}

/// Turn a registry-append result into a status object. A failed registry write
/// must not fail the snapshot itself (the pin already exists); the caller learns
/// the write did not land so it can retry discovery rather than lose the id.
fn registry_write_status(result: Result<(), WorkbenchToolError>) -> Value {
    match result {
        Ok(()) => {
            json!({"written": true, "path_relative": format!("metadata/{CHECKPOINT_REGISTRY_RELPATH}")})
        }
        Err(err) => json!({
            "written": false,
            "path_relative": format!("metadata/{CHECKPOINT_REGISTRY_RELPATH}"),
            "error": err.to_string(),
        }),
    }
}

fn read_checkpoint_registry<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
) -> Result<Vec<Value>, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    validate_workbench_id(id)?;
    let path = checkpoint_registry_path(options, id);
    if stat_path_or_absent(client, &path)?.is_none() {
        return Ok(Vec::new());
    }
    let bytes = client.cat(&path).map_err(client_error)?;
    let text = String::from_utf8(bytes).map_err(|err| {
        WorkbenchToolError::new(format!("checkpoint registry is not valid UTF-8: {err}"))
    })?;
    let mut entries = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // A truncated or malformed trailing line (e.g. an interrupted append) is
        // skipped rather than failing the whole listing.
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            entries.push(value);
        }
    }
    Ok(entries)
}

/// Latest registered `snapshot_id` for a checkpoint name. The registry is
/// append-only, so a re-minted name yields several rows; the newest wins.
fn resolve_name_to_snapshot_id(entries: &[Value], name: &str) -> Option<u64> {
    entries.iter().rev().find_map(|entry| {
        if entry.get("name").and_then(Value::as_str) == Some(name) {
            entry.get("snapshot_id").and_then(Value::as_u64)
        } else {
            None
        }
    })
}

/// Name most recently registered for a snapshot id, for carrying the alias
/// forward onto renew rows. Returns null when the snapshot has no named row.
fn registry_name_for_snapshot<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    snapshot_id: u64,
) -> Result<Value, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let entries = read_checkpoint_registry(client, options, id)?;
    let name = entries.iter().rev().find_map(|entry| {
        if entry.get("snapshot_id").and_then(Value::as_u64) == Some(snapshot_id) {
            entry.get("name").filter(|value| !value.is_null()).cloned()
        } else {
            None
        }
    });
    Ok(name.unwrap_or(Value::Null))
}

/// Resolve the `at_snapshot` argument (a numeric id or a checkpoint name) to a
/// snapshot id. A name is looked up in the workbench registry.
fn resolve_at_snapshot<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    value: &Value,
) -> Result<u64, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    match value {
        Value::Number(number) => number.as_u64().ok_or_else(|| {
            WorkbenchToolError::new(
                "at_snapshot must be a non-negative snapshot id or a checkpoint name",
            )
        }),
        Value::String(name) => {
            validate_snapshot_name(name)?;
            let entries = read_checkpoint_registry(client, options, id)?;
            resolve_name_to_snapshot_id(&entries, name).ok_or_else(|| {
                WorkbenchToolError::new(format!(
                    "unknown checkpoint name {name} for workbench {id}; run workbench_snapshot_list to see checkpoints"
                ))
            })
        }
        _ => Err(WorkbenchToolError::new(
            "at_snapshot must be a snapshot id (integer) or a checkpoint name (string)",
        )),
    }
}

/// Resolve the renew target: exactly one of `snapshot_id` (numeric) or `name`
/// (registry-resolved) must be given.
fn resolve_renew_target<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    args: &Value,
) -> Result<u64, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let snapshot_id = optional_u64(args, "snapshot_id")?;
    let name = optional_string(args, "name")?;
    match (snapshot_id, name) {
        (Some(snapshot_id), None) => Ok(snapshot_id),
        (None, Some(name)) => {
            validate_snapshot_name(name)?;
            let entries = read_checkpoint_registry(client, options, id)?;
            resolve_name_to_snapshot_id(&entries, name).ok_or_else(|| {
                WorkbenchToolError::new(format!(
                    "unknown checkpoint name {name} for workbench {id}; run workbench_snapshot_list to see checkpoints"
                ))
            })
        }
        (Some(_), Some(_)) | (None, None) => Err(WorkbenchToolError::new(
            "provide exactly one of snapshot_id or name",
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkbenchPathScope {
    path: String,
    section: Option<String>,
    relative_path: Option<String>,
}

#[derive(Debug)]
struct WorkbenchManifestSummary {
    committed: bool,
    manifest_path: Option<String>,
    manifest_metadata: Option<PathMetadata>,
    manifest_text: Option<String>,
    envelope: Option<Value>,
    include_manifest: bool,
}

impl WorkbenchManifestSummary {
    fn matches_manifest_pattern(&self, pattern: &str) -> bool {
        let Some(text) = &self.manifest_text else {
            return false;
        };
        text.to_ascii_lowercase()
            .contains(&pattern.to_ascii_lowercase())
    }

    fn into_json(self, options: &WorkbenchMcpOptions, id: &str) -> Value {
        let body = self
            .manifest_metadata
            .as_ref()
            .and_then(|metadata| metadata.body.as_ref());
        let envelope = self.envelope.unwrap_or(Value::Null);
        let manifest = if self.include_manifest {
            envelope.clone()
        } else {
            Value::Null
        };
        json!({
            "workbench_id": id,
            "path": workbench_path(options, id),
            "committed": self.committed,
            "manifest_path": self.manifest_path,
            "manifest_size_bytes": self.manifest_metadata.as_ref().map(|metadata| metadata.attr.size),
            "manifest_generation": self.manifest_metadata.as_ref().map(|metadata| metadata.attr.generation),
            "manifest_digest_uri": body.map(|body| body.digest_uri.clone()),
            "manifest_summary": manifest_summary_json(&envelope),
            "manifest": manifest,
        })
    }
}

fn path_scope(
    options: &WorkbenchMcpOptions,
    id: &str,
    path: &str,
) -> Result<WorkbenchPathScope, WorkbenchToolError> {
    scoped_path(options, id, path, false)
}

/// Scope for paths returned by enumeration (list entries, grep matches).
/// Other NoKV clients share the namespace and may have written entries
/// outside the standard sections; those are surfaced with `section: null`
/// and a workbench-relative path instead of failing the whole call.
fn enumerated_path_scope(
    options: &WorkbenchMcpOptions,
    id: &str,
    path: &str,
) -> Result<WorkbenchPathScope, WorkbenchToolError> {
    scoped_path(options, id, path, true)
}

fn scoped_path(
    options: &WorkbenchMcpOptions,
    id: &str,
    path: &str,
    tolerate_non_section: bool,
) -> Result<WorkbenchPathScope, WorkbenchToolError> {
    let base = workbench_path(options, id);
    if path == base {
        return Ok(WorkbenchPathScope {
            path: path.to_owned(),
            section: None,
            relative_path: None,
        });
    }
    let prefix = format!("{base}/");
    let rest = path.strip_prefix(&prefix).ok_or_else(|| {
        WorkbenchToolError::new(format!("path {path} is outside workbench {base}"))
    })?;
    let first = rest.split('/').next().unwrap_or(rest);
    if let Err(err) = validate_section(first) {
        if !tolerate_non_section {
            return Err(err);
        }
        return Ok(WorkbenchPathScope {
            path: path.to_owned(),
            section: None,
            relative_path: Some(rest.to_owned()),
        });
    }
    let (section, relative_path) = match rest.split_once('/') {
        Some((section, relative_path)) => (section.to_owned(), Some(relative_path.to_owned())),
        None => (rest.to_owned(), None),
    };
    Ok(WorkbenchPathScope {
        path: path.to_owned(),
        section: Some(section),
        relative_path,
    })
}

fn compact_list_entry(
    options: &WorkbenchMcpOptions,
    id: &str,
    entry: &Value,
) -> Result<Value, WorkbenchToolError> {
    let path = entry
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| WorkbenchToolError::new("list entry missing path"))?;
    let scope = enumerated_path_scope(options, id, path)?;
    Ok(json!({
        "name": entry.get("name").cloned().unwrap_or(Value::Null),
        "path": scope.path,
        "section": scope.section,
        "relative_path": scope.relative_path,
        "kind": entry.get("kind").cloned().unwrap_or(Value::Null),
        "size_bytes": entry.get("size_bytes").cloned().unwrap_or(Value::Null),
        "entry_count": entry.get("entry_count").cloned().unwrap_or(Value::Null),
    }))
}

fn compact_stat_card(scope: &WorkbenchPathScope, card: &Value, metadata: &PathMetadata) -> Value {
    let body = metadata.body.as_ref();
    json!({
        "name": card.get("name").cloned().unwrap_or(Value::Null),
        "path": scope.path.clone(),
        "section": scope.section.clone(),
        "relative_path": scope.relative_path.clone(),
        "kind": card.get("kind").cloned().unwrap_or(Value::Null),
        "size_bytes": card.get("size_bytes").cloned().unwrap_or(Value::Null),
        "entry_count": card.get("entry_count").cloned().unwrap_or(Value::Null),
        "record_count": card.get("record_count").cloned().unwrap_or(Value::Null),
        "inode": metadata.attr.inode.get(),
        "generation": metadata.attr.generation,
        "content_type": body.map(|body| body.content_type.clone()),
        "digest_uri": body.map(|body| body.digest_uri.clone()),
        "producer": body.map(|body| body.producer.clone()),
        "manifest_id": body.map(|body| body.manifest_id.clone()),
    })
}

fn compact_grep_match(
    options: &WorkbenchMcpOptions,
    id: &str,
    match_: &Value,
) -> Result<Value, WorkbenchToolError> {
    let path = match_
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| WorkbenchToolError::new("grep match missing path"))?;
    let scope = enumerated_path_scope(options, id, path)?;
    Ok(json!({
        "path": scope.path,
        "section": scope.section,
        "relative_path": scope.relative_path,
        "line_number": match_.get("line_number").cloned().unwrap_or(Value::Null),
        "snippet": match_.get("snippet").cloned().unwrap_or(Value::Null),
    }))
}

fn workbench_manifest_summary<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    include_manifest: bool,
) -> Result<WorkbenchManifestSummary, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let manifest_path = section_path(options, id, "metadata", Some("run_manifest.json"));
    // An out-of-band directory under the root has no metadata/ section at all;
    // the missing-ancestor NotFound folds into "uncommitted" like a missing
    // manifest file does (find must tolerate such entries the way ls does).
    let Some(metadata) = stat_path_or_absent(client, &manifest_path)? else {
        return Ok(WorkbenchManifestSummary {
            committed: false,
            manifest_path: None,
            manifest_metadata: None,
            manifest_text: None,
            envelope: None,
            include_manifest,
        });
    };
    let bytes = client.cat(&manifest_path).map_err(client_error)?;
    let text = String::from_utf8(bytes).map_err(|err| {
        WorkbenchToolError::new(format!("run manifest is not valid UTF-8: {err}"))
    })?;
    let envelope = serde_json::from_str::<Value>(&text)
        .map_err(|err| WorkbenchToolError::new(format!("run manifest is not valid JSON: {err}")))?;
    Ok(WorkbenchManifestSummary {
        committed: true,
        manifest_path: Some(manifest_path),
        manifest_metadata: Some(metadata),
        manifest_text: Some(text),
        envelope: Some(envelope),
        include_manifest,
    })
}

fn manifest_summary_json(envelope: &Value) -> Value {
    if envelope.is_null() {
        return Value::Null;
    }
    let manifest_keys = envelope
        .get("manifest")
        .and_then(Value::as_object)
        .map(|object| {
            let mut keys = object.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            keys
        })
        .unwrap_or_default();
    json!({
        "schema": envelope.get("schema").cloned().unwrap_or(Value::Null),
        "workbench_id": envelope.get("workbench_id").cloned().unwrap_or(Value::Null),
        "committed_at_unix_seconds": envelope
            .get("committed_at_unix_seconds")
            .cloned()
            .unwrap_or(Value::Null),
        "manifest_keys": manifest_keys,
        "manifest_task": envelope
            .get("manifest")
            .and_then(|manifest| manifest.get("task"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn ensure_standard_dirs<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
) -> Result<(), WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    client
        .metadata()
        .bootstrap_root(DEFAULT_MODE_DIR, options.uid, options.gid)
        .map_err(client_error)?;
    ensure_dir_path(client, options, &options.root)?;
    let base = workbench_path(options, id);
    ensure_dir_path(client, options, &base)?;
    for section in SECTIONS {
        ensure_dir_path(client, options, &section_path(options, id, section, None))?;
    }
    Ok(())
}

fn ensure_parent_dirs<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    id: &str,
    section: &str,
    rel_path: &str,
) -> Result<(), WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let mut components: Vec<&str> = rel_path.split('/').collect();
    components.pop();
    if components.is_empty() {
        return Ok(());
    }
    let mut current = String::new();
    for component in components {
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(component);
        let path = section_path(options, id, section, Some(&current));
        ensure_dir_path(client, options, &path)?;
    }
    Ok(())
}

/// `stat_path` that folds "a path component does not exist" into `Ok(None)`.
/// The metadata server reports a missing *ancestor* as a `NotFound` error
/// while a missing leaf is `Ok(None)`; probes asking "is this subtree
/// materialized yet" (a multi-level per-agent root, a workbench's manifest
/// under an out-of-band directory) treat both as plain absence.
fn stat_path_or_absent<O>(
    client: &NoKvFsClient<O>,
    path: &str,
) -> Result<Option<PathMetadata>, WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    match client.metadata().stat_path(path) {
        Ok(metadata) => Ok(metadata),
        Err(err) if is_metadata_not_found(&err) => Ok(None),
        Err(err) => Err(client_error(err)),
    }
}

/// Ensure `path` exists as a directory, creating any missing ancestors
/// (mkdir -p). `mkdir` is non-recursive, so a multi-level workbench root such
/// as `/agents/<agent_id>/wb` (per-agent tenant isolation) requires each
/// ancestor to be created in turn — otherwise the first create fails with
/// "metadata entry not found".
fn ensure_dir_path<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    path: &str,
) -> Result<(), WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    let mut current = String::new();
    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        current.push('/');
        current.push_str(component);
        ensure_single_dir(client, options, &current)?;
    }
    Ok(())
}

/// Ensure one path component exists as a directory. Idempotent: if a concurrent
/// creator wins the race (e.g. two agents/daemons materializing the shared
/// ancestors of their roots at once), a re-stat confirming the directory now
/// exists is treated as success rather than surfacing the CAS conflict.
fn ensure_single_dir<O>(
    client: &NoKvFsClient<O>,
    options: &WorkbenchMcpOptions,
    path: &str,
) -> Result<(), WorkbenchToolError>
where
    O: ObjectStore + Send + Sync + 'static,
{
    if let Some(metadata) = client.metadata().stat_path(path).map_err(client_error)? {
        if metadata.attr.file_type == FileType::Directory {
            return Ok(());
        }
        return Err(WorkbenchToolError::new(format!(
            "path exists but is not a directory: {path}"
        )));
    }
    match client
        .metadata()
        .mkdir(path, DEFAULT_MODE_DIR, options.uid, options.gid)
    {
        Ok(_) => Ok(()),
        Err(err) => {
            if let Some(metadata) = client.metadata().stat_path(path).map_err(client_error)? {
                if metadata.attr.file_type == FileType::Directory {
                    return Ok(());
                }
            }
            Err(client_error(err))
        }
    }
}

fn required_workbench_id(args: &Value) -> Result<String, WorkbenchToolError> {
    let id = required_string(args, "id")?;
    validate_workbench_id(id)?;
    Ok(id.to_owned())
}

fn validate_workbench_id(id: &str) -> Result<(), WorkbenchToolError> {
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return Err(WorkbenchToolError::new("id must not be empty"));
    };
    if !first.is_ascii_alphanumeric() {
        return Err(WorkbenchToolError::new(
            "id must start with an ASCII letter or digit",
        ));
    }
    if id.len() > 128 {
        return Err(WorkbenchToolError::new("id must be at most 128 bytes"));
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err(WorkbenchToolError::new(
            "id may contain only ASCII letters, digits, '_' and '-'",
        ));
    }
    Ok(())
}

fn required_section(args: &Value) -> Result<&str, WorkbenchToolError> {
    let section = required_string(args, "section")?;
    validate_section(section)?;
    Ok(section)
}

fn validate_section(section: &str) -> Result<(), WorkbenchToolError> {
    if SECTIONS.contains(&section) {
        Ok(())
    } else {
        Err(WorkbenchToolError::new(format!(
            "invalid section {section}; expected one of {}",
            SECTIONS.join(", ")
        )))
    }
}

fn scoped_path_from_optional_args(
    options: &WorkbenchMcpOptions,
    id: &str,
    args: &Value,
) -> Result<String, WorkbenchToolError> {
    let section = optional_string(args, "section")?;
    let rel_path = optional_string(args, "path")?;
    match (section, rel_path) {
        (None, None) => Ok(workbench_path(options, id)),
        (None, Some("")) => Ok(workbench_path(options, id)),
        (None, Some(_)) => Err(WorkbenchToolError::new(
            "path requires section when scoped below a workbench",
        )),
        (Some(section), path) => {
            validate_section(section)?;
            let rel = match path {
                Some(raw) => {
                    let rel_path = normalize_relative_path(raw, "path", true)?;
                    reject_section_prefixed_path(section, &rel_path, "path")?;
                    Some(rel_path)
                }
                None => None,
            };
            Ok(section_path(options, id, section, rel.as_deref()))
        }
    }
}

fn required_section_relative_path(
    args: &Value,
    section: &str,
    field: &'static str,
) -> Result<String, WorkbenchToolError> {
    let rel_path = required_relative_path(args, field)?;
    reject_section_prefixed_path(section, &rel_path, field)?;
    Ok(rel_path)
}

fn required_relative_path(args: &Value, field: &'static str) -> Result<String, WorkbenchToolError> {
    normalize_relative_path(required_string(args, field)?, field, false)
}

fn reject_section_prefixed_path(
    section: &str,
    rel_path: &str,
    field: &'static str,
) -> Result<(), WorkbenchToolError> {
    let section_prefix = format!("{section}/");
    if rel_path == section || rel_path.starts_with(&section_prefix) {
        return Err(WorkbenchToolError::new(format!(
            "{field} must be relative to section {section}; do not prefix it with {section}/"
        )));
    }
    Ok(())
}

fn normalize_relative_path(
    raw: &str,
    field: &'static str,
    allow_empty: bool,
) -> Result<String, WorkbenchToolError> {
    if raw.is_empty() {
        if allow_empty {
            return Ok(String::new());
        }
        return Err(WorkbenchToolError::new(format!(
            "{field} must not be empty"
        )));
    }
    if raw.starts_with('/') {
        return Err(WorkbenchToolError::new(format!("{field} must be relative")));
    }
    if raw.ends_with('/') {
        return Err(WorkbenchToolError::new(format!(
            "{field} must not end with '/'"
        )));
    }
    if raw.contains("//") {
        return Err(WorkbenchToolError::new(format!(
            "{field} must not contain empty components"
        )));
    }
    if raw.contains('\\') {
        return Err(WorkbenchToolError::new(format!(
            "{field} must use POSIX separators"
        )));
    }
    if raw.contains('\0') {
        return Err(WorkbenchToolError::new(format!(
            "{field} contains a NUL byte"
        )));
    }
    for component in raw.split('/') {
        if component == "." || component == ".." {
            return Err(WorkbenchToolError::new(format!(
                "{field} must not contain '.' or '..' components"
            )));
        }
    }
    Ok(raw.to_owned())
}

fn normalize_absolute_path(raw: &str, field: &'static str) -> Result<String, String> {
    if raw.is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    if !raw.starts_with('/') {
        return Err(format!("{field} must be an absolute NoKV path"));
    }
    if raw.contains('\\') {
        return Err(format!("{field} must use POSIX separators"));
    }
    if raw.contains('\0') {
        return Err(format!("{field} contains a NUL byte"));
    }
    let trimmed = raw.trim_end_matches('/');
    let path = if trimmed.is_empty() { "/" } else { trimmed };
    let mut components = Vec::new();
    for component in path.trim_start_matches('/').split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(format!("{field} must not contain '.' or '..' components"));
        }
        components.push(component);
    }
    if components.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

fn workbench_path(options: &WorkbenchMcpOptions, id: &str) -> String {
    format!("{}/{}", options.root, id)
}

fn section_path(
    options: &WorkbenchMcpOptions,
    id: &str,
    section: &str,
    rel_path: Option<&str>,
) -> String {
    let base = format!("{}/{section}", workbench_path(options, id));
    match rel_path {
        Some(path) if !path.is_empty() => format!("{base}/{path}"),
        _ => base,
    }
}

fn payload_bytes(
    args: &Value,
    max_bytes: usize,
) -> Result<(Vec<u8>, &'static str), WorkbenchToolError> {
    let text = optional_string(args, "text")?;
    let encoded = optional_string(args, "base64")?;
    let (bytes, content_type) = match (text, encoded) {
        (Some(text), Some("")) if !text.is_empty() => {
            (text.as_bytes().to_vec(), "text/plain; charset=utf-8")
        }
        (Some(""), Some(encoded)) if !encoded.is_empty() => (
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|err| WorkbenchToolError::new(format!("invalid base64 payload: {err}")))?,
            "application/octet-stream",
        ),
        (Some(_), Some(_)) => {
            return Err(WorkbenchToolError::new(
                "provide exactly one of text or base64",
            ))
        }
        (Some(text), None) => (text.as_bytes().to_vec(), "text/plain; charset=utf-8"),
        (None, Some(encoded)) => (
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|err| WorkbenchToolError::new(format!("invalid base64 payload: {err}")))?,
            "application/octet-stream",
        ),
        (None, None) => {
            return Err(WorkbenchToolError::new(
                "provide exactly one of text or base64",
            ))
        }
    };
    if bytes.len() > max_bytes {
        return Err(WorkbenchToolError::new(format!(
            "payload exceeds max_bytes: {} > {max_bytes}",
            bytes.len()
        )));
    }
    Ok((bytes, content_type))
}

fn artifact_metadata(
    options: &WorkbenchMcpOptions,
    path: &str,
    digest_uri: &str,
    content_type: &str,
) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: "nokv-workbench-mcp".to_owned(),
        digest_uri: digest_uri.to_owned(),
        content_type: content_type.to_owned(),
        manifest_id: path.trim_start_matches('/').to_owned(),
        mode: DEFAULT_MODE_FILE,
        uid: options.uid,
        gid: options.gid,
    }
}

fn digest_uri(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn required_string<'a>(args: &'a Value, name: &'static str) -> Result<&'a str, WorkbenchToolError> {
    args.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| WorkbenchToolError::new(format!("missing required string argument {name}")))
}

fn optional_string<'a>(
    args: &'a Value,
    name: &'static str,
) -> Result<Option<&'a str>, WorkbenchToolError> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_str().map(Some).ok_or_else(|| {
            WorkbenchToolError::new(format!("{name} must be a string when provided"))
        }),
    }
}

fn optional_bool(args: &Value, name: &'static str) -> Result<Option<bool>, WorkbenchToolError> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_bool().map(Some).ok_or_else(|| {
            WorkbenchToolError::new(format!("{name} must be a boolean when provided"))
        }),
    }
}

fn optional_u64(args: &Value, name: &'static str) -> Result<Option<u64>, WorkbenchToolError> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            WorkbenchToolError::new(format!("{name} must be an integer when provided"))
        }),
    }
}

fn optional_usize(args: &Value, name: &'static str) -> Result<Option<usize>, WorkbenchToolError> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let raw = value.as_u64().ok_or_else(|| {
                WorkbenchToolError::new(format!("{name} must be an integer when provided"))
            })?;
            usize::try_from(raw).map(Some).map_err(|_| {
                WorkbenchToolError::new(format!("{name} is too large for this platform"))
            })
        }
    }
}

fn client_error(err: ClientError) -> WorkbenchToolError {
    match &err {
        ClientError::Metadata(MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms,
            now_ms,
        }) => WorkbenchToolError::typed(
            "SnapshotLeaseExpired",
            err.to_string(),
            false,
            json!({
                "snapshot_id": snapshot_id,
                "lease_expires_unix_ms": lease_expires_unix_ms,
                "now_ms": now_ms,
            }),
        ),
        ClientError::Metadata(MetadError::SnapshotRootMismatch {
            snapshot_id,
            expected_root,
            actual_root,
            actual_shard,
        }) => WorkbenchToolError::typed(
            "SnapshotRootMismatch",
            err.to_string(),
            false,
            json!({
                "snapshot_id": snapshot_id,
                "expected_root": expected_root.get(),
                "actual_root": actual_root.map(|root| root.get()),
                "actual_shard": actual_shard,
            }),
        ),
        ClientError::Metadata(MetadError::SnapshotBindingChanged { root_path }) => {
            WorkbenchToolError::typed(
                "SnapshotBindingChanged",
                err.to_string(),
                true,
                json!({"root_path": root_path}),
            )
        }
        ClientError::Metadata(MetadError::SnapshotRenewContended {
            snapshot_id,
            attempts,
        }) => WorkbenchToolError::typed(
            "SnapshotRenewContended",
            err.to_string(),
            true,
            json!({"snapshot_id": snapshot_id, "attempts": attempts}),
        ),
        _ => WorkbenchToolError::typed("NoKvClientError", err.to_string(), false, json!({})),
    }
}

fn snapshot_client_error(snapshot_id: u64, err: ClientError) -> WorkbenchToolError {
    if matches!(&err, ClientError::Metadata(MetadError::NotFound)) {
        return WorkbenchToolError::typed(
            "SnapshotNotFound",
            format!("snapshot {snapshot_id} not found; it may have been reaped after lease expiry"),
            false,
            json!({"snapshot_id": snapshot_id}),
        );
    }
    client_error(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_workbench_root() {
        assert_eq!(
            normalize_workbench_root("/workbenches/").unwrap(),
            "/workbenches"
        );
        assert!(normalize_workbench_root("relative").is_err());
        assert!(normalize_workbench_root("/").is_err());
        assert!(normalize_workbench_root("/work/../benches").is_err());
    }

    #[test]
    fn validates_relative_paths() {
        assert_eq!(
            normalize_relative_path("plots/plot_001.png", "path", false).unwrap(),
            "plots/plot_001.png"
        );
        assert_eq!(normalize_relative_path("", "path", true).unwrap(), "");
        assert!(normalize_relative_path("", "path", false).is_err());
        assert!(normalize_relative_path("../escape", "path", false).is_err());
        assert!(normalize_relative_path("/escape", "path", false).is_err());
        assert!(normalize_relative_path("bad//path", "path", false).is_err());
        assert!(normalize_relative_path(".", "path", false).is_err());
        assert!(normalize_relative_path("bad\\path", "path", false).is_err());
        assert!(normalize_relative_path("dir/", "path", false).is_err());
        assert!(normalize_relative_path("bad\0path", "path", false).is_err());
    }

    #[test]
    fn validates_workbench_ids() {
        assert!(validate_workbench_id("spedas-task-001").is_ok());
        assert!(validate_workbench_id("_bad").is_err());
        assert!(validate_workbench_id("bad/path").is_err());
    }

    #[test]
    fn write_conflict_exhausted_keeps_inner_error_and_retry_guidance() {
        let message = write_conflict_exhausted(6, "metadata predicate failed").to_string();
        assert!(
            message.contains("conflicted with concurrent writers after 6 attempts; retry the call"),
            "message: {message}"
        );
        assert!(
            message.contains("metadata predicate failed"),
            "message: {message}"
        );
    }

    #[test]
    fn split_piped_grep_pattern_enforces_alternative_cap() {
        let forwarded_for = |pattern: String| {
            let mut map = serde_json::Map::new();
            map.insert("pattern".to_owned(), Value::String(pattern));
            map
        };
        let join = |count: usize| {
            (0..count)
                .map(|index| format!("alt-{index}"))
                .collect::<Vec<_>>()
                .join("|")
        };

        let mut sixteen = forwarded_for(join(16));
        split_piped_grep_pattern(&mut sixteen).unwrap();
        assert_eq!(sixteen["patterns"].as_array().unwrap().len(), 16);

        let mut seventeen = forwarded_for(join(17));
        let error = split_piped_grep_pattern(&mut seventeen)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(
                "pattern contains 17 '|'-separated alternatives; at most 16 are supported"
            ),
            "error: {error}"
        );
    }

    #[test]
    fn empty_catalog_result_mirrors_agent_catalog_shape() {
        let result = empty_query_result("catalog", "/workbenches");
        assert_eq!(result["status"], "success");
        assert_eq!(result["path"], "/workbenches");
        assert_eq!(result["catalog_empty"], true);
        assert_eq!(result["catalog"]["filterable"], json!([]));
        assert_eq!(result["catalog"]["sortable"], json!([]));
        assert_eq!(result["catalog"]["facetable"], json!([]));
        assert_eq!(result["catalog"]["facets"], json!([]));
        assert_eq!(result["child_catalogs"], json!([]));
    }

    #[test]
    fn scopes_enumerated_paths_outside_sections_as_null_section() {
        let options = WorkbenchMcpOptions {
            root: "/workbenches".to_owned(),
            max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
            uid: 1000,
            gid: 1000,
        };
        let scope = |path| enumerated_path_scope(&options, "wb", path).unwrap();
        assert_eq!(
            scope("/workbenches/wb/outputs/plot.png"),
            WorkbenchPathScope {
                path: "/workbenches/wb/outputs/plot.png".to_owned(),
                section: Some("outputs".to_owned()),
                relative_path: Some("plot.png".to_owned()),
            }
        );
        assert_eq!(
            scope("/workbenches/wb/note.txt"),
            WorkbenchPathScope {
                path: "/workbenches/wb/note.txt".to_owned(),
                section: None,
                relative_path: Some("note.txt".to_owned()),
            }
        );
        assert_eq!(
            scope("/workbenches/wb/junk/scratch.txt"),
            WorkbenchPathScope {
                path: "/workbenches/wb/junk/scratch.txt".to_owned(),
                section: None,
                relative_path: Some("junk/scratch.txt".to_owned()),
            }
        );
        assert!(enumerated_path_scope(&options, "wb", "/elsewhere/file").is_err());
        // The strict variant used for request targets keeps rejecting.
        assert!(path_scope(&options, "wb", "/workbenches/wb/junk/scratch.txt").is_err());
        assert!(path_scope(&options, "wb", "/workbenches/wb/note.txt").is_err());
    }
}
