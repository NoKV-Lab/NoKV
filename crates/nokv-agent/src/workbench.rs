use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::mcp::McpToolSurface;
use crate::{execute_agent_tool, AgentFs, AgentIndexError, AgentStore, AgentToolDefinition};

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

pub fn tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "workbench_create",
            description: "Create an agent-native workbench directory with standard sections.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"}
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
                    "path": {"type": "string"},
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
            description: "List a workbench, section, or subdirectory.",
            parameters: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"]},
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
                    "path": {"type": ["string", "null"]}
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
                    "path": {"type": "string"},
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
            description: "Search workbench file bodies for a case-insensitive literal substring.",
            parameters: json!({
                "type": "object",
                "required": ["id", "pattern", "recursive"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": ["string", "null"], "enum": ["input", "scripts", "outputs", "logs", "metadata", null]},
                    "path": {"type": ["string", "null"]},
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
                    "committed": {"type": ["boolean", "null"]},
                    "manifest_pattern": {"type": ["string", "null"]},
                    "include_manifest": {"type": "boolean"},
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
    if !replace && fs.node(&path)?.is_some() {
        return Err(WorkbenchToolError::new(format!(
            "path already exists; set replace=true to overwrite: {path}"
        )));
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
    shape_read_tool_result(options, &id, &target, read_tool, result)
}

fn shape_read_tool_result(
    options: &WorkbenchMcpOptions,
    id: &str,
    target: &str,
    read_tool: &str,
    result: Value,
) -> Result<Value, WorkbenchToolError> {
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
            "entries": result.get("entries").cloned().unwrap_or_else(|| json!([])),
            "next_cursor": result.get("next_cursor").cloned().unwrap_or(Value::Null),
            "truncated": result.get("truncated").cloned().unwrap_or(Value::Bool(false)),
        })),
        "stat" => Ok(json!({
            "status": "success",
            "workbench_id": id,
            "workbench_path": workbench_path(options, id),
            "section": scope.section,
            "relative_path": scope.relative_path,
            "path": scope.path,
            "card": result.get("card").cloned().unwrap_or(Value::Null),
        })),
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
            "matches": result.get("matches").cloned().unwrap_or_else(|| json!([])),
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
    let offset = optional_string(args, "cursor")?
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(0);
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
            "next_cursor": Value::Null,
            "truncated": false,
        }));
    }
    let mut matches = Vec::new();
    for node in fs.list(&options.root, None, usize::MAX)?.0 {
        if node.kind != crate::AgentNodeKind::Directory {
            continue;
        }
        let id = node.name.clone();
        let summary = workbench_manifest_summary(fs, options, &id, include_manifest)?;
        if let Some(committed) = committed_filter {
            if summary.committed != committed {
                continue;
            }
        }
        if let Some(pattern) = manifest_pattern {
            let Some(text) = summary.manifest_text.as_deref() else {
                continue;
            };
            if !text.to_lowercase().contains(&pattern.to_lowercase()) {
                continue;
            }
        }
        matches.push(summary_json(options, &id, summary));
    }
    let match_count = matches.len();
    let page = matches
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next_offset = offset.saturating_add(page.len());
    Ok(json!({
        "status": "success",
        "path": options.root,
        "matches": page,
        "match_count": match_count,
        "next_cursor": (next_offset < match_count).then(|| next_offset.to_string()),
        "truncated": next_offset < match_count,
    }))
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
        .and_then(Value::as_object)
        .ok_or_else(|| WorkbenchToolError::new("missing object argument manifest"))?;
    let replace = optional_bool(args, "replace")?.unwrap_or(false);
    ensure_standard_dirs(fs, options, &id)?;
    let path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    if !replace && fs.node(&path)?.is_some() {
        return Err(WorkbenchToolError::new(
            "run_manifest.json already exists; set replace=true to overwrite",
        ));
    }
    let envelope = json!({
        "schema": "nokv.agent.workbench.run_manifest.v0",
        "backend": "nokv-agent",
        "workbench_id": id,
        "workbench_path": workbench_path(options, &id),
        "committed_at_unix_seconds": unix_seconds(),
        "manifest": manifest,
    });
    let bytes = serde_json::to_vec_pretty(&envelope)
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    let digest_uri = digest_uri(&bytes);
    fs.put_file(&path, bytes, Some("application/json".to_owned()))?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "manifest_path": path,
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
    let manifest = fs
        .read_file(&manifest_path)
        .map_err(|_| WorkbenchToolError::new("workbench must be committed before snapshot"))?;
    let files = fs.files_under(&workbench_path(options, &id), true)?;
    let mut hasher = Sha256::new();
    hasher.update(&manifest);
    let file_manifest = files
        .into_iter()
        .filter(|node| node.kind == crate::AgentNodeKind::File)
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
    let snapshot_id = u64::from_be_bytes(digest[0..8].try_into().unwrap());
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
        "workbench_id": id,
        "workbench_path": workbench_path(options, &id),
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
        "snapshot_id": snapshot_id,
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
            manifest_text: None,
            envelope: None,
            include_manifest,
        });
    };
    let text = String::from_utf8(bytes)
        .map_err(|err| WorkbenchToolError::new(format!("manifest is not UTF-8: {err}")))?;
    let envelope = serde_json::from_str::<Value>(&text)
        .map_err(|err| WorkbenchToolError::new(format!("manifest is not JSON: {err}")))?;
    Ok(WorkbenchManifestSummary {
        committed: true,
        manifest_path: Some(manifest_path),
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
    if raw.contains('\\') || raw.contains('\0') {
        return Err(WorkbenchToolError::new(format!(
            "{field} must use POSIX separators and contain no NUL bytes"
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
    if raw.contains('\\') || raw.contains('\0') {
        return Err(format!(
            "{field} must use POSIX separators and contain no NUL bytes"
        ));
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
            "path escaped workbench root: {path}"
        )));
    };
    let (section, relative_path) = match rest.split_once('/') {
        Some((section, relative_path)) => {
            validate_section(section)?;
            (section.to_owned(), Some(relative_path.to_owned()))
        }
        None => {
            validate_section(rest)?;
            (rest.to_owned(), None)
        }
    };
    Ok(WorkbenchPathScope {
        path: path.to_owned(),
        section: Some(section),
        relative_path,
    })
}

fn payload_bytes(
    args: &Value,
    max_bytes: usize,
) -> Result<(Vec<u8>, &'static str), WorkbenchToolError> {
    let text = optional_string(args, "text")?;
    let encoded = optional_string(args, "base64")?;
    let (bytes, content_type) = match (text, encoded) {
        (Some(text), None) => (text.as_bytes().to_vec(), "text/plain; charset=utf-8"),
        (None, Some(encoded)) => (
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|err| WorkbenchToolError::new(format!("invalid base64 payload: {err}")))?,
            "application/octet-stream",
        ),
        _ => {
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
            responses[2]["result"]["structuredContent"]["items"][0]["value"]["event"],
            "flare"
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
}
