use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::mcp::McpToolSurface;
use crate::tool::execute_agent_tool;
use crate::{AgentFs, AgentIndexError, AgentNodeKind, AgentStore, AgentToolDefinition};

pub const DEFAULT_WORKBENCH_ROOT: &str = "/workbenches";
pub const DEFAULT_WORKBENCH_MAX_BYTES: usize = 16 * 1024 * 1024;

const DEFAULT_FIND_LIMIT: usize = 50;
const MAX_FIND_LIMIT: usize = 100;
const SECTIONS: &[&str] = &["input", "scripts", "outputs", "logs", "metadata"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchMcpOptions {
    pub root: String,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkbenchToolError {
    message: String,
}

impl WorkbenchToolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for WorkbenchToolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for WorkbenchToolError {}

impl From<AgentIndexError> for WorkbenchToolError {
    fn from(err: AgentIndexError) -> Self {
        Self::new(err.to_string())
    }
}

pub struct WorkbenchMcpSurface<'a, S> {
    fs: &'a AgentFs<S>,
    options: WorkbenchMcpOptions,
}

impl<'a, S> WorkbenchMcpSurface<'a, S> {
    pub fn new(fs: &'a AgentFs<S>, options: WorkbenchMcpOptions) -> Self {
        Self { fs, options }
    }
}

impl<S> McpToolSurface for WorkbenchMcpSurface<'_, S>
where
    S: AgentStore,
{
    fn tool_definitions(&self) -> Vec<AgentToolDefinition> {
        tool_definitions()
    }

    fn execute_tool(&self, name: &str, args: &Value) -> Result<Value, String> {
        execute_tool(self.fs, &self.options, name, args).map_err(|err| err.to_string())
    }
}

pub fn normalize_workbench_root(raw: &str) -> Result<String, String> {
    let normalized = normalize_absolute_path(raw, "workbench_root")?;
    if normalized == "/" {
        return Err("workbench_root must not be /".to_owned());
    }
    Ok(normalized)
}

// Parameter schemas below must stay byte-identical to the DFS-backed
// definitions in crates/nokv/src/bin/nokv/workbench_mcp.rs; the golden
// constants in tests/workbench_parity.rs pin that contract.
pub fn tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "workbench_create",
            description: "Create an agent-native workbench directory with standard sections.",
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
            description: "Write one file into a workbench section.",
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
            name: "workbench_list",
            description: "List a workbench, section, or subdirectory. Not recursive. Entries written outside the standard sections through the embedded agent store are returned with section null and cannot be addressed by the other workbench tools.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_stat",
            description: "Inspect a workbench path compact card.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_read",
            description: "Read one workbench file.",
            parameters: json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": SECTIONS},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_grep",
            description: "Search workbench file bodies for a case-insensitive literal substring. This is not regex grep. Matches in files written outside the standard sections through the embedded agent store are returned with section null and cannot be addressed by the other workbench tools.",
            parameters: json!({
                "type": "object",
                "required": ["id", "pattern", "recursive"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "pattern": {"type": "string"},
                    "recursive": {"type": "boolean"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_find",
            description: "List workbenches with optional committed-state and manifest filters.",
            parameters: json!({
                "type": "object",
                "properties": {
                    "committed": {"type": ["boolean", "null"], "description": "Filter by completion marker. Null or omitted returns all workbenches."},
                    "manifest_pattern": {"type": ["string", "null"], "description": "Case-insensitive literal substring filter over metadata/run_manifest.json."},
                    "include_manifest": {"type": "boolean", "description": "Include full run_manifest.json envelopes. Defaults false."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_FIND_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "workbench_commit",
            description: "Mark a workbench complete by writing metadata/run_manifest.json.",
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
            description: "Write an agent-native logical snapshot record for a committed workbench.",
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

pub fn execute_tool<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    name: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    match name {
        "workbench_create" => create_workbench(fs, options, args),
        "workbench_put_file" => put_file(fs, options, args),
        "workbench_list" => execute_read_tool(fs, options, "ls", args),
        "workbench_stat" => execute_read_tool(fs, options, "stat", args),
        "workbench_read" => execute_read_tool(fs, options, "read", args),
        "workbench_grep" => execute_read_tool(fs, options, "grep", args),
        "workbench_find" => find_workbenches(fs, options, args),
        "workbench_commit" => commit_workbench(fs, options, args),
        "workbench_snapshot" => snapshot_workbench(fs, options, args),
        other => Err(WorkbenchToolError::new(format!(
            "unknown workbench tool {other}"
        ))),
    }
}

fn create_workbench<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let id = required_workbench_id(args)?;
    ensure_standard_dirs(fs, options, &id)?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": workbench_path(options, &id),
        "sections": SECTIONS,
        "backend": "nokv-agent",
    }))
}

fn put_file<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let id = required_workbench_id(args)?;
    let section = required_section(args)?;
    let rel_path = required_section_relative_path(args, section, "path")?;
    let replace = optional_bool(args, "replace")?.unwrap_or(false);
    let (bytes, default_content_type) = payload_bytes(args, options.max_bytes)?;
    let content_type = optional_string(args, "content_type")?
        .unwrap_or(default_content_type)
        .to_owned();

    ensure_standard_dirs(fs, options, &id)?;
    ensure_parent_dirs(fs, options, &id, section, &rel_path)?;
    let path = section_path(options, &id, section, Some(&rel_path));
    if let Some(existing) = fs.node(&path)? {
        if existing.kind != AgentNodeKind::File {
            return Err(WorkbenchToolError::new(format!(
                "path exists but is not a file: {path}"
            )));
        }
        if !replace {
            return Err(WorkbenchToolError::new(format!(
                "path already exists; set replace=true to overwrite: {path}"
            )));
        }
    }
    let digest_uri = digest_uri(&bytes);
    let size_bytes = bytes.len();
    fs.put_file(&path, bytes, Some(content_type.clone()))?;

    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "section": section,
        "relative_path": rel_path,
        "path": path,
        "size_bytes": size_bytes,
        "inode": Value::Null,
        "generation": Value::Null,
        "digest_uri": digest_uri,
        "content_type": content_type,
        "replace": replace,
        "backend": "nokv-agent",
    }))
}

fn execute_read_tool<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    read_tool: &str,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
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
        }
        "grep" => {
            forwarded.remove("format");
            forwarded.remove("offset");
        }
        _ => {}
    }
    let result = execute_agent_tool(fs, read_tool, &Value::Object(forwarded))
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    shape_read_tool_result(fs, options, &id, &target, read_tool, result)
}

fn shape_read_tool_result<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    id: &str,
    target: &str,
    read_tool: &str,
    result: Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let scope = path_scope(options, id, target)?;
    match read_tool {
        "ls" => Ok(json!({
            "status": "success",
            "workbench_id": id,
            "workbench_path": workbench_path(options, id),
            "section": scope.section,
            "relative_path": scope.relative_path,
            "path": scope.path,
            "entry_count": result.get("entry_count").cloned().unwrap_or(Value::Null),
            "entries": result
                .get("entries")
                .and_then(Value::as_array)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| compact_list_entry(options, id, entry))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .map(Value::Array)
                .unwrap_or_else(|| json!([])),
            "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
            "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
        })),
        "stat" => {
            let node = fs.node(&scope.path)?.ok_or_else(|| {
                WorkbenchToolError::new(format!("path not found: {}", scope.path))
            })?;
            let card = compact_stat_card(fs, &scope, &node)?;
            Ok(json!({
                "status": "success",
                "workbench_id": id,
                "workbench_path": workbench_path(options, id),
                "section": scope.section,
                "relative_path": scope.relative_path,
                "path": scope.path,
                "card": card,
            }))
        }
        "read" => Ok(json!({
            "status": "success",
            "workbench_id": id,
            "workbench_path": workbench_path(options, id),
            "section": scope.section,
            "relative_path": scope.relative_path,
            "path": scope.path,
            "total_size_bytes": result.get("total_size_bytes").cloned().unwrap_or(Value::Null),
            "format": result.get("format").cloned().unwrap_or(Value::Null),
            "record_type": result.get("record_type").cloned().unwrap_or(Value::Null),
            "record_count": result.get("record_count").cloned().unwrap_or(Value::Null),
            "cursor": result.get("cursor").cloned().unwrap_or(Value::Null),
            "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
            "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
            "items": result.get("items").cloned().unwrap_or_else(|| json!([])),
            "bytes": result.get("bytes").cloned().unwrap_or(Value::Null),
        })),
        "grep" => Ok(json!({
            "status": "success",
            "workbench_id": id,
            "workbench_path": workbench_path(options, id),
            "section": scope.section,
            "relative_path": scope.relative_path,
            "path": scope.path,
            "pattern": result.get("pattern").cloned().unwrap_or(Value::Null),
            "recursive": result.get("recursive").cloned().unwrap_or(Value::Bool(false)),
            "matches": result
                .get("matches")
                .and_then(Value::as_array)
                .map(|matches| {
                    matches
                        .iter()
                        .map(|match_| compact_grep_match(options, id, match_))
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .map(Value::Array)
                .unwrap_or_else(|| json!([])),
            "files_scanned": result.get("files_scanned").cloned().unwrap_or(Value::Null),
            "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
            "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
        })),
        other => Err(WorkbenchToolError::new(format!(
            "unsupported read tool {other}"
        ))),
    }
}

fn find_workbenches<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let committed_filter = optional_bool(args, "committed")?;
    let manifest_pattern = optional_string(args, "manifest_pattern")?;
    let include_manifest = optional_bool(args, "include_manifest")?.unwrap_or(false);
    let cursor = optional_string(args, "cursor")?;
    if let Some(raw) = cursor {
        validate_list_cursor(raw)?;
    }
    let limit = optional_usize(args, "limit")?.unwrap_or(DEFAULT_FIND_LIMIT);
    if limit == 0 || limit > MAX_FIND_LIMIT {
        return Err(WorkbenchToolError::new(format!(
            "limit must be between 1 and {MAX_FIND_LIMIT}"
        )));
    }
    if fs.node(&options.root)?.is_none() {
        return Ok(json!({
            "status": "success",
            "path": options.root,
            "matches": [],
            "match_count": 0,
            "entry_count": 0,
            "next_cursor": Value::Null,
            "truncated": false,
        }));
    }
    // DFS-shaped pagination: page the root directory listing, then filter
    // only the current page. match_count is per page, so a page may hold
    // fewer than `limit` matches (or none) while next_cursor is non-null.
    let list_args = json!({
        "path": options.root,
        "cursor": cursor,
        "limit": limit,
    });
    let page = execute_agent_tool(fs, "ls", &list_args)
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
        let summary = workbench_manifest_summary(fs, options, id, include_manifest)?;
        if let Some(committed) = committed_filter {
            if summary.committed != committed {
                continue;
            }
        }
        if let Some(pattern) = manifest_pattern {
            let Some(text) = summary.manifest_text.as_deref() else {
                continue;
            };
            if !text
                .to_ascii_lowercase()
                .contains(&pattern.to_ascii_lowercase())
            {
                continue;
            }
        }
        matches.push(summary_json(options, id, summary));
    }
    let match_count = matches.len();
    Ok(json!({
        "status": "success",
        "path": options.root,
        "matches": matches,
        "match_count": match_count,
        "entry_count": page.get("entry_count").cloned().unwrap_or(Value::Null),
        "next_cursor": page.get("next_cursor").cloned().unwrap_or(Value::Null),
        "truncated": page.get("truncated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

/// The underlying `ls` cursor is a hex-encoded store key. Reject malformed
/// cursors before forwarding so a bad cursor can never silently restart the
/// listing from the first page.
fn validate_list_cursor(raw: &str) -> Result<(), WorkbenchToolError> {
    let valid = raw.len().is_multiple_of(2) && raw.bytes().all(|byte| byte.is_ascii_hexdigit());
    if valid {
        Ok(())
    } else {
        Err(WorkbenchToolError::new(format!("invalid cursor {raw}")))
    }
}

fn commit_workbench<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
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
    ensure_standard_dirs(fs, options, &id)?;
    let path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    if !replace && fs.node(&path)?.is_some() {
        return Err(WorkbenchToolError::new(
            "run_manifest.json already exists; set replace=true to overwrite",
        ));
    }
    // Same data-at-rest schema as the DFS-backed workbench so one consumer
    // can parse manifests from either backend; "backend" marks lineage.
    let envelope = json!({
        "schema": "nokv.workbench.run_manifest.v0",
        "backend": "nokv-agent",
        "workbench_id": id,
        "workbench_path": workbench_path(options, &id),
        "committed_at_unix_seconds": unix_seconds(),
        "manifest": manifest,
    });
    let bytes = serde_json::to_vec_pretty(&envelope)
        .map_err(|err| WorkbenchToolError::new(format!("failed to encode manifest: {err}")))?;
    let digest_uri = digest_uri(&bytes);
    let size_bytes = bytes.len();
    fs.put_file(&path, bytes, Some("application/json".to_owned()))?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": path,
        "size_bytes": size_bytes,
        "inode": Value::Null,
        "generation": Value::Null,
        "digest_uri": digest_uri,
        "replace": replace,
        "backend": "nokv-agent",
    }))
}

fn snapshot_workbench<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    args: &Value,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let id = required_workbench_id(args)?;
    let manifest_path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    if fs.node(&manifest_path)?.is_none() {
        return Err(WorkbenchToolError::new(format!(
            "workbench {id} is not committed; missing {manifest_path}"
        )));
    }
    let manifest = fs.read_file(&manifest_path)?;
    let path = workbench_path(options, &id);
    // Prior snapshot records are excluded from the digest and file manifest
    // so re-snapshotting unchanged content yields the same snapshot id.
    let snapshots_prefix = format!(
        "{}/",
        section_path(options, &id, "metadata", Some("snapshots"))
    );
    let files = fs.files_under(&path, true)?;
    let mut hasher = Sha256::new();
    hasher.update(&manifest);
    let file_manifest = files
        .into_iter()
        .filter(|node| {
            node.kind == crate::AgentNodeKind::File && !node.path.starts_with(&snapshots_prefix)
        })
        .map(|node| {
            let bytes = fs.read_file(&node.path)?;
            hasher.update(node.path.as_bytes());
            hasher.update(&bytes);
            Ok(json!({
                "path": node.path,
                "size_bytes": bytes.len(),
                "digest_uri": digest_uri(&bytes),
            }))
        })
        .collect::<Result<Vec<_>, AgentIndexError>>()?;
    let digest = hasher.finalize();
    // Keep the id within 2^53 so JSON consumers with IEEE-754 numbers do not
    // lose precision; the full digest travels in snapshot_digest.
    let snapshot_id = u64::from_be_bytes(digest[0..8].try_into().unwrap()) & ((1 << 53) - 1);
    let snapshot_digest = format!("sha256:{digest:x}");
    let snapshot_path = section_path(
        options,
        &id,
        "metadata",
        Some(&format!("snapshots/{snapshot_id}.json")),
    );
    let snapshot_record = json!({
        "schema": "nokv.agent.workbench.snapshot.v0",
        "backend": "nokv-agent",
        "snapshot_kind": "logical",
        "snapshot_id": snapshot_id,
        "snapshot_digest": snapshot_digest,
        "workbench_id": id,
        "workbench_path": path,
        "manifest_path": manifest_path,
        "created_at_unix_seconds": unix_seconds(),
        "files": file_manifest,
    });
    let bytes = serde_json::to_vec_pretty(&snapshot_record)
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    fs.put_file(&snapshot_path, bytes, Some("application/json".to_owned()))?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": path,
        "snapshot_id": snapshot_id,
        "snapshot_digest": snapshot_digest,
        "snapshot_kind": "logical",
        "snapshot_path": snapshot_path,
        "read_version": Value::Null,
        "backend": "nokv-agent",
    }))
}

#[derive(Clone, Debug)]
struct WorkbenchManifestSummary {
    committed: bool,
    manifest_path: Option<String>,
    manifest_size_bytes: Option<usize>,
    manifest_digest_uri: Option<String>,
    manifest_text: Option<String>,
    envelope: Option<Value>,
    include_manifest: bool,
}

#[derive(Clone, Debug)]
struct WorkbenchPathScope {
    path: String,
    section: Option<String>,
    relative_path: Option<String>,
}

fn workbench_manifest_summary<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    id: &str,
    include_manifest: bool,
) -> Result<WorkbenchManifestSummary, WorkbenchToolError>
where
    S: AgentStore,
{
    let manifest_path = section_path(options, id, "metadata", Some("run_manifest.json"));
    let Ok(bytes) = fs.read_file(&manifest_path) else {
        return Ok(WorkbenchManifestSummary {
            committed: false,
            manifest_path: None,
            manifest_size_bytes: None,
            manifest_digest_uri: None,
            manifest_text: None,
            envelope: None,
            include_manifest,
        });
    };
    let manifest_size_bytes = bytes.len();
    let manifest_digest_uri = digest_uri(&bytes);
    let text = String::from_utf8(bytes).map_err(|err| {
        WorkbenchToolError::new(format!("run manifest is not valid UTF-8: {err}"))
    })?;
    let envelope = serde_json::from_str::<Value>(&text)
        .map_err(|err| WorkbenchToolError::new(format!("run manifest is not valid JSON: {err}")))?;
    Ok(WorkbenchManifestSummary {
        committed: true,
        manifest_path: Some(manifest_path),
        manifest_size_bytes: Some(manifest_size_bytes),
        manifest_digest_uri: Some(manifest_digest_uri),
        manifest_text: Some(text),
        envelope: Some(envelope),
        include_manifest,
    })
}

fn summary_json(
    options: &WorkbenchMcpOptions,
    id: &str,
    summary: WorkbenchManifestSummary,
) -> Value {
    let manifest_summary = summary
        .envelope
        .as_ref()
        .map(manifest_summary_json)
        .unwrap_or(Value::Null);
    json!({
        "workbench_id": id,
        "path": workbench_path(options, id),
        "committed": summary.committed,
        "manifest_path": summary.manifest_path,
        "manifest_size_bytes": summary.manifest_size_bytes,
        // The embedded store has no write generations; digest_uri is the
        // stable cross-backend change signal.
        "manifest_generation": Value::Null,
        "manifest_digest_uri": summary.manifest_digest_uri,
        "manifest_summary": manifest_summary,
        "manifest": if summary.include_manifest { summary.envelope } else { None },
    })
}

fn manifest_summary_json(envelope: &Value) -> Value {
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

fn ensure_standard_dirs<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    id: &str,
) -> Result<(), WorkbenchToolError>
where
    S: AgentStore,
{
    fs.create_dir_all(&options.root)?;
    fs.create_dir_all(&workbench_path(options, id))?;
    for section in SECTIONS {
        fs.create_dir_all(&section_path(options, id, section, None))?;
    }
    Ok(())
}

fn ensure_parent_dirs<S>(
    fs: &AgentFs<S>,
    options: &WorkbenchMcpOptions,
    id: &str,
    section: &str,
    rel_path: &str,
) -> Result<(), WorkbenchToolError>
where
    S: AgentStore,
{
    let Some((parent, _)) = rel_path.rsplit_once('/') else {
        return Ok(());
    };
    fs.create_dir_all(&section_path(options, id, section, Some(parent)))?;
    Ok(())
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
    let rel_path = normalize_relative_path(required_string(args, field)?, field, false)?;
    reject_section_prefixed_path(section, &rel_path, field)?;
    Ok(rel_path)
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
        return Err(format!("{field} must be an absolute agent path"));
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

fn path_scope(
    options: &WorkbenchMcpOptions,
    id: &str,
    path: &str,
) -> Result<WorkbenchPathScope, WorkbenchToolError> {
    scoped_path(options, id, path, false)
}

/// Scope for paths returned by enumeration (list entries, grep matches).
/// Other embedders of the agent store share the namespace and may have
/// written entries outside the standard sections; those are surfaced with
/// `section: null` and a workbench-relative path instead of failing the
/// whole call.
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
    let Some(rest) = path.strip_prefix(&prefix) else {
        return Err(WorkbenchToolError::new(format!(
            "path {path} is outside workbench {base}"
        )));
    };
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

/// Same card shape as the DFS-backed workbench stat. Fields the embedded
/// store has no concept of (inode, generation, record_count) are explicit
/// nulls so consumers can share one parser across backends.
fn compact_stat_card<S>(
    fs: &AgentFs<S>,
    scope: &WorkbenchPathScope,
    node: &crate::AgentNode,
) -> Result<Value, WorkbenchToolError>
where
    S: AgentStore,
{
    let is_file = node.kind == AgentNodeKind::File;
    let entry_count = if is_file {
        Value::Null
    } else {
        json!(fs.list(&scope.path, None, usize::MAX)?.0.len())
    };
    // The embedded store does not persist digests; hash the body on read.
    // Bodies are bounded by the workbench max_bytes write path.
    let digest = if is_file {
        json!(digest_uri(&fs.read_file(&scope.path)?))
    } else {
        Value::Null
    };
    let content_type = if is_file {
        json!(node
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned()))
    } else {
        Value::Null
    };
    Ok(json!({
        "name": node.name,
        "path": scope.path,
        "section": scope.section,
        "relative_path": scope.relative_path,
        "kind": if is_file { "file" } else { "directory" },
        "size_bytes": node.size_bytes,
        "entry_count": entry_count,
        "record_count": Value::Null,
        "inode": Value::Null,
        "generation": Value::Null,
        "content_type": content_type,
        "digest_uri": digest,
        "producer": is_file.then_some("nokv-agent"),
        "manifest_id": is_file.then(|| scope.path.trim_start_matches('/').to_owned()),
    }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{run_mcp_surface_stream, AgentId, HoltAgentStore};

    fn surface() -> (AgentFs<HoltAgentStore>, WorkbenchMcpOptions) {
        let fs = AgentFs::new(
            AgentId::new("workbench-test"),
            HoltAgentStore::open_memory().unwrap(),
        );
        fs.bootstrap().unwrap();
        let options = WorkbenchMcpOptions {
            root: DEFAULT_WORKBENCH_ROOT.to_owned(),
            max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
        };
        (fs, options)
    }

    fn run_requests(requests: Vec<Value>) -> Vec<Value> {
        let (fs, options) = surface();
        let surface = WorkbenchMcpSurface::new(&fs, options);
        let input = requests
            .into_iter()
            .map(|value| serde_json::to_string(&value).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let mut writer = Vec::new();
        run_mcp_surface_stream(&surface, std::io::Cursor::new(input), &mut writer).unwrap();
        String::from_utf8(writer)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn tools_are_workbench_scoped() {
        let names = tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "workbench_create",
                "workbench_put_file",
                "workbench_list",
                "workbench_stat",
                "workbench_read",
                "workbench_grep",
                "workbench_find",
                "workbench_commit",
                "workbench_snapshot",
            ]
        );
    }

    #[test]
    fn create_put_read_commit_snapshot_flow() {
        let responses = run_requests(vec![
            json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workbench_create","arguments":{"id":"task-1"}}}),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workbench_put_file","arguments":{"id":"task-1","section":"input","path":"event.json","text":"{\"event\":\"flare\"}"}}}),
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workbench_read","arguments":{"id":"task-1","section":"input","path":"event.json","format":"structured"}}}),
            json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"workbench_commit","arguments":{"id":"task-1","manifest":{"task":"flare-search"}}}}),
            json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"workbench_snapshot","arguments":{"id":"task-1"}}}),
        ]);
        assert_eq!(
            responses[0]["result"]["structuredContent"]["status"],
            "success"
        );
        assert_eq!(
            responses[1]["result"]["structuredContent"]["path"],
            "/workbenches/task-1/input/event.json"
        );
        assert_eq!(
            responses[2]["result"]["structuredContent"]["record_type"],
            "json_object"
        );
        assert_eq!(
            responses[2]["result"]["structuredContent"]["items"][0]["value"],
            json!({"key": "event", "value": "flare"})
        );
        assert_eq!(
            responses[3]["result"]["structuredContent"]["backend"],
            "nokv-agent"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["snapshot_kind"],
            "logical"
        );
    }

    #[test]
    fn put_file_payload_tolerates_empty_counterpart() {
        let (fs, options) = surface();
        // (text, base64="") writes the text payload.
        let response = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"a.txt","text":"x","base64":""}),
        )
        .unwrap();
        assert_eq!(response["size_bytes"], 1);
        assert_eq!(response["content_type"], "text/plain; charset=utf-8");
        // (text="", base64) decodes the base64 payload.
        let response = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"b.bin","text":"","base64":"aGk="}),
        )
        .unwrap();
        assert_eq!(response["size_bytes"], 2);
        assert_eq!(response["content_type"], "application/octet-stream");
        assert_eq!(
            fs.read_file("/workbenches/task-1/input/b.bin").unwrap(),
            b"hi".to_vec()
        );
        // A lone empty text still writes an empty file.
        let response = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"c.txt","text":""}),
        )
        .unwrap();
        assert_eq!(response["size_bytes"], 0);
        // Ambiguous combinations keep failing.
        for args in [
            json!({"id":"task-1","section":"input","path":"d","text":"","base64":""}),
            json!({"id":"task-1","section":"input","path":"d","text":"x","base64":"eQ=="}),
            json!({"id":"task-1","section":"input","path":"d"}),
        ] {
            let err = execute_tool(&fs, &options, "workbench_put_file", &args).unwrap_err();
            assert_eq!(err.to_string(), "provide exactly one of text or base64");
        }
    }

    #[test]
    fn put_file_reports_null_inode_and_generation() {
        let (fs, options) = surface();
        let response = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"a.txt","text":"x"}),
        )
        .unwrap();
        assert_eq!(response["inode"], Value::Null);
        assert_eq!(response["generation"], Value::Null);
    }

    #[test]
    fn commit_response_and_envelope_align_with_dfs() {
        let (fs, options) = surface();
        let response = execute_tool(
            &fs,
            &options,
            "workbench_commit",
            &json!({"id":"task-1","manifest":{"task":"flare-search"}}),
        )
        .unwrap();
        assert_eq!(
            response["path"],
            "/workbenches/task-1/metadata/run_manifest.json"
        );
        let bytes = fs
            .read_file("/workbenches/task-1/metadata/run_manifest.json")
            .unwrap();
        assert_eq!(response["size_bytes"], bytes.len());
        assert_eq!(response["inode"], Value::Null);
        assert_eq!(response["generation"], Value::Null);
        assert_eq!(response["digest_uri"], digest_uri(&bytes));
        assert!(response.get("manifest_path").is_none());
        let envelope: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(envelope["schema"], "nokv.workbench.run_manifest.v0");
        assert_eq!(envelope["backend"], "nokv-agent");
        assert_eq!(envelope["manifest"]["task"], "flare-search");
    }

    #[test]
    fn commit_validates_manifest_argument() {
        let (fs, options) = surface();
        let err =
            execute_tool(&fs, &options, "workbench_commit", &json!({"id":"task-1"})).unwrap_err();
        assert_eq!(err.to_string(), "missing required argument manifest");
        let err = execute_tool(
            &fs,
            &options,
            "workbench_commit",
            &json!({"id":"task-1","manifest":"str"}),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "manifest must be a JSON object");
    }

    #[test]
    fn find_pages_root_listing_like_dfs() {
        let (fs, options) = surface();
        for index in 1..=5 {
            execute_tool(
                &fs,
                &options,
                "workbench_create",
                &json!({"id": format!("wb-{index}")}),
            )
            .unwrap();
        }
        for index in 1..=3 {
            execute_tool(
                &fs,
                &options,
                "workbench_commit",
                &json!({"id": format!("wb-{index}"), "manifest": {"task": "Flare-Search"}}),
            )
            .unwrap();
        }
        // Page through the root listing; match_count counts the current page.
        let mut cursor = Value::Null;
        let mut total = 0;
        let mut pages = 0;
        loop {
            let page = execute_tool(
                &fs,
                &options,
                "workbench_find",
                &json!({"cursor": cursor, "limit": 2}),
            )
            .unwrap();
            let matches = page["matches"].as_array().unwrap();
            assert!(matches.len() <= 2);
            assert_eq!(page["match_count"], matches.len());
            assert!(page["entry_count"].is_number());
            total += matches.len();
            pages += 1;
            cursor = page["next_cursor"].clone();
            if cursor.is_null() {
                assert_eq!(page["truncated"], false);
                break;
            }
        }
        assert_eq!(total, 5);
        assert_eq!(pages, 3);
        // committed filter drops uncommitted workbenches within each page.
        let committed = execute_tool(
            &fs,
            &options,
            "workbench_find",
            &json!({"committed": true, "limit": 100}),
        )
        .unwrap();
        assert_eq!(committed["match_count"], 3);
        let uncommitted = execute_tool(
            &fs,
            &options,
            "workbench_find",
            &json!({"committed": false, "limit": 100}),
        )
        .unwrap();
        assert_eq!(uncommitted["match_count"], 2);
        // Manifest pattern matching is ASCII case-insensitive.
        let pattern = execute_tool(
            &fs,
            &options,
            "workbench_find",
            &json!({"manifest_pattern": "flare-search", "limit": 100}),
        )
        .unwrap();
        assert_eq!(pattern["match_count"], 3);
    }

    #[test]
    fn find_rejects_malformed_cursor() {
        let (fs, options) = surface();
        execute_tool(&fs, &options, "workbench_create", &json!({"id":"wb-1"})).unwrap();
        let err = execute_tool(
            &fs,
            &options,
            "workbench_find",
            &json!({"cursor": "garbage"}),
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "invalid cursor garbage");
        let err =
            execute_tool(&fs, &options, "workbench_find", &json!({"cursor": "zz"})).unwrap_err();
        assert_eq!(err.to_string(), "invalid cursor zz");
    }

    #[test]
    fn find_empty_root_reports_entry_count() {
        let (fs, options) = surface();
        let response = execute_tool(&fs, &options, "workbench_find", &json!({})).unwrap();
        assert_eq!(response["match_count"], 0);
        assert_eq!(response["entry_count"], 0);
        assert_eq!(response["next_cursor"], Value::Null);
        assert_eq!(response["truncated"], false);
    }

    #[test]
    fn find_reports_manifest_summary_fields() {
        let (fs, options) = surface();
        execute_tool(&fs, &options, "workbench_create", &json!({"id":"wb-1"})).unwrap();
        let commit = execute_tool(
            &fs,
            &options,
            "workbench_commit",
            &json!({"id":"wb-1","manifest":{"task":"flare"}}),
        )
        .unwrap();
        let found = execute_tool(&fs, &options, "workbench_find", &json!({})).unwrap();
        let entry = &found["matches"][0];
        let bytes = fs
            .read_file("/workbenches/wb-1/metadata/run_manifest.json")
            .unwrap();
        assert_eq!(entry["manifest_size_bytes"], bytes.len());
        assert_eq!(entry["manifest_digest_uri"], commit["digest_uri"]);
        assert_eq!(entry["manifest_generation"], Value::Null);
        assert_eq!(
            entry["manifest_summary"]["schema"],
            "nokv.workbench.run_manifest.v0"
        );
        // Uncommitted workbenches report the same keys as nulls.
        execute_tool(&fs, &options, "workbench_create", &json!({"id":"wb-2"})).unwrap();
        let found = execute_tool(&fs, &options, "workbench_find", &json!({})).unwrap();
        let entry = found["matches"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["workbench_id"] == "wb-2")
            .unwrap();
        assert_eq!(entry["manifest_size_bytes"], Value::Null);
        assert_eq!(entry["manifest_digest_uri"], Value::Null);
        assert_eq!(entry["manifest_generation"], Value::Null);
    }

    #[test]
    fn stat_returns_compact_card() {
        let (fs, options) = surface();
        let put = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"data.bin","base64":"aGk="}),
        )
        .unwrap();
        let stat = execute_tool(
            &fs,
            &options,
            "workbench_stat",
            &json!({"id":"task-1","section":"input","path":"data.bin"}),
        )
        .unwrap();
        let card = &stat["card"];
        assert_eq!(card["name"], "data.bin");
        assert_eq!(card["path"], "/workbenches/task-1/input/data.bin");
        assert_eq!(card["section"], "input");
        assert_eq!(card["relative_path"], "data.bin");
        assert_eq!(card["kind"], "file");
        assert_eq!(card["size_bytes"], 2);
        assert_eq!(card["entry_count"], Value::Null);
        assert_eq!(card["record_count"], Value::Null);
        assert_eq!(card["inode"], Value::Null);
        assert_eq!(card["generation"], Value::Null);
        assert_eq!(card["content_type"], "application/octet-stream");
        assert_eq!(card["digest_uri"], put["digest_uri"]);
        assert_eq!(card["producer"], "nokv-agent");
        assert_eq!(card["manifest_id"], "workbenches/task-1/input/data.bin");
        for leaked in ["schema", "sample", "catalog", "indexed_values", "body"] {
            assert!(card.get(leaked).is_none(), "card leaks {leaked}");
        }
        // Workbench root card counts direct children.
        let stat = execute_tool(&fs, &options, "workbench_stat", &json!({"id":"task-1"})).unwrap();
        let card = &stat["card"];
        assert_eq!(card["kind"], "directory");
        assert_eq!(card["entry_count"], SECTIONS.len());
        assert_eq!(card["section"], Value::Null);
        assert_eq!(card["digest_uri"], Value::Null);
        assert_eq!(card["producer"], Value::Null);
        assert_eq!(card["manifest_id"], Value::Null);
    }

    #[test]
    fn snapshot_is_idempotent_and_stays_within_53_bits() {
        let (fs, options) = surface();
        execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"outputs","path":"result.txt","text":"done"}),
        )
        .unwrap();
        execute_tool(
            &fs,
            &options,
            "workbench_commit",
            &json!({"id":"task-1","manifest":{"task":"flare"}}),
        )
        .unwrap();
        let first =
            execute_tool(&fs, &options, "workbench_snapshot", &json!({"id":"task-1"})).unwrap();
        let second =
            execute_tool(&fs, &options, "workbench_snapshot", &json!({"id":"task-1"})).unwrap();
        assert_eq!(first["snapshot_id"], second["snapshot_id"]);
        assert_eq!(first["snapshot_digest"], second["snapshot_digest"]);
        let id = first["snapshot_id"].as_u64().unwrap();
        assert!(id < (1 << 53));
        let digest = first["snapshot_digest"].as_str().unwrap();
        assert!(digest.starts_with("sha256:"));
        assert_eq!(first["path"], "/workbenches/task-1");
        assert_eq!(first["read_version"], Value::Null);
        // The record file name uses the same 53-bit id.
        let snapshot_path = first["snapshot_path"].as_str().unwrap();
        assert_eq!(
            snapshot_path,
            format!("/workbenches/task-1/metadata/snapshots/{id}.json")
        );
        let record: Value = serde_json::from_slice(&fs.read_file(snapshot_path).unwrap()).unwrap();
        assert_eq!(record["snapshot_id"], id);
        // Prior snapshot records are excluded from the file manifest.
        let files = record["files"].as_array().unwrap();
        assert!(files.iter().all(|file| !file["path"]
            .as_str()
            .unwrap()
            .contains("/metadata/snapshots/")));
    }

    #[test]
    fn snapshot_requires_commit_with_dfs_message() {
        let (fs, options) = surface();
        execute_tool(&fs, &options, "workbench_create", &json!({"id":"task-1"})).unwrap();
        let err =
            execute_tool(&fs, &options, "workbench_snapshot", &json!({"id":"task-1"})).unwrap_err();
        assert_eq!(
            err.to_string(),
            "workbench task-1 is not committed; missing /workbenches/task-1/metadata/run_manifest.json"
        );
    }

    #[test]
    fn path_validation_messages_align_with_dfs() {
        assert_eq!(
            normalize_relative_path("bad\\path", "path", false)
                .unwrap_err()
                .to_string(),
            "path must use POSIX separators"
        );
        assert_eq!(
            normalize_relative_path("bad\0path", "path", false)
                .unwrap_err()
                .to_string(),
            "path contains a NUL byte"
        );
        assert_eq!(
            normalize_absolute_path("relative", "workbench_root").unwrap_err(),
            "workbench_root must be an absolute agent path"
        );
        assert_eq!(
            normalize_absolute_path("/a\\b", "workbench_root").unwrap_err(),
            "workbench_root must use POSIX separators"
        );
        assert_eq!(
            normalize_absolute_path("/a\0b", "workbench_root").unwrap_err(),
            "workbench_root contains a NUL byte"
        );
        let options = WorkbenchMcpOptions {
            root: DEFAULT_WORKBENCH_ROOT.to_owned(),
            max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
        };
        assert_eq!(
            path_scope(&options, "wb", "/elsewhere/file")
                .unwrap_err()
                .to_string(),
            "path /elsewhere/file is outside workbench /workbenches/wb"
        );
    }

    #[test]
    fn rejects_path_escape_and_default_overwrite() {
        let (fs, options) = surface();
        assert!(execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"../x","text":"bad"})
        )
        .is_err());
        execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"event.txt","text":"one"}),
        )
        .unwrap();
        assert!(execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"event.txt","text":"two"})
        )
        .is_err());
    }

    #[test]
    fn put_file_rejects_directory_targets_and_file_ancestors() {
        let (fs, options) = surface();
        execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"sub/leaf.txt","text":"one"}),
        )
        .unwrap();

        // Overwriting an existing directory must fail even with replace=true.
        let err = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"sub","text":"x","replace":true}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("is not a file"),
            "unexpected error: {err}"
        );

        // Writing beneath an existing file must fail instead of silently
        // turning the file into a directory.
        let err = execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({"id":"task-1","section":"input","path":"sub/leaf.txt/nested.txt","text":"x"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("is not a directory"),
            "unexpected error: {err}"
        );

        // The original file must still be readable afterwards.
        let read = execute_tool(
            &fs,
            &options,
            "workbench_read",
            &json!({"id":"task-1","section":"input","path":"sub/leaf.txt"}),
        )
        .unwrap();
        assert_eq!(read["path"], "/workbenches/task-1/input/sub/leaf.txt");
    }

    #[test]
    fn list_and_grep_tolerate_out_of_band_entries() {
        let (fs, options) = surface();
        execute_tool(&fs, &options, "workbench_create", &json!({"id":"task-oob"})).unwrap();
        execute_tool(
            &fs,
            &options,
            "workbench_put_file",
            &json!({
                "id":"task-oob",
                "section":"outputs",
                "path":"spectrum.csv",
                "text":"freq,power\n1,2\n",
                "content_type":"text/csv"
            }),
        )
        .unwrap();
        // Out-of-band writes through the embedded API, outside the sections.
        fs.put_file(
            "/workbenches/task-oob/note.txt",
            b"scratch note".to_vec(),
            None,
        )
        .unwrap();
        fs.put_file(
            "/workbenches/task-oob/junk/scratch.txt",
            b"freq rogue\n".to_vec(),
            None,
        )
        .unwrap();

        let list =
            execute_tool(&fs, &options, "workbench_list", &json!({"id":"task-oob"})).unwrap();
        let entries = list["entries"].as_array().unwrap();
        let entry = |name: &str| {
            entries
                .iter()
                .find(|entry| entry["name"] == name)
                .unwrap_or_else(|| panic!("missing list entry {name}: {entries:?}"))
        };
        for section in SECTIONS {
            assert_eq!(entry(section)["section"], *section);
        }
        assert_eq!(entry("note.txt")["section"], Value::Null);
        assert_eq!(entry("note.txt")["relative_path"], "note.txt");
        assert_eq!(entry("junk")["section"], Value::Null);
        assert_eq!(entry("junk")["relative_path"], "junk");

        let grep = execute_tool(
            &fs,
            &options,
            "workbench_grep",
            &json!({"id":"task-oob","pattern":"freq","recursive":true}),
        )
        .unwrap();
        let matches = grep["matches"].as_array().unwrap();
        let match_for = |path: &str| {
            matches
                .iter()
                .find(|match_| match_["path"] == path)
                .unwrap_or_else(|| panic!("missing grep match {path}: {matches:?}"))
        };
        let section_match = match_for("/workbenches/task-oob/outputs/spectrum.csv");
        assert_eq!(section_match["section"], "outputs");
        assert_eq!(section_match["relative_path"], "spectrum.csv");
        let alien_match = match_for("/workbenches/task-oob/junk/scratch.txt");
        assert_eq!(alien_match["section"], Value::Null);
        assert_eq!(alien_match["relative_path"], "junk/scratch.txt");

        // Request targets stay strict: out-of-band entries are unaddressable.
        assert!(execute_tool(
            &fs,
            &options,
            "workbench_stat",
            &json!({"id":"task-oob","section":"junk","path":"scratch.txt"})
        )
        .is_err());
    }
}
