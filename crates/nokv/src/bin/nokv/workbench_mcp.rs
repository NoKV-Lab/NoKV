use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use nokv_agent::AgentToolDefinition;
use nokv_client::{ArtifactMetadata, NoKvFsClient};
use nokv_object::ObjectStore;
use nokv_types::FileType;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{DEFAULT_MODE_DIR, DEFAULT_MODE_FILE};

pub const DEFAULT_WORKBENCH_MAX_BYTES: usize = 16 * 1024 * 1024;

const SECTIONS: &[&str] = &["input", "scripts", "outputs", "logs", "metadata"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchMcpOptions {
    pub root: String,
    pub max_bytes: usize,
    pub uid: u32,
    pub gid: u32,
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
            name: "nokv_workbench_create",
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
            name: "nokv_workbench_put_file",
            description:
                "Publish one file into a jailed workbench section. Paths are relative to the section; overwrite requires replace=true.",
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
            name: "nokv_workbench_list",
            description:
                "List a workbench, section, or subdirectory through the NoKV namespace. Not recursive.",
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
            name: "nokv_workbench_stat",
            description:
                "Inspect a workbench, section, subdirectory, or file compact card through the NoKV namespace.",
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
            name: "nokv_workbench_read",
            description:
                "Read one workbench file through the NoKV namespace. Structured mode returns JSON, YAML, or text records; bytes mode returns byte ranges.",
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
            name: "nokv_workbench_grep",
            description:
                "Search workbench file bodies for a case-insensitive literal substring. This is not regex grep.",
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
            name: "nokv_workbench_commit",
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
            name: "nokv_workbench_snapshot",
            description:
                "Snapshot a committed workbench subtree and return the NoKV snapshot id and read version.",
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
        "nokv_workbench_create" => create_workbench(client, options, args),
        "nokv_workbench_put_file" => put_file(client, options, args),
        "nokv_workbench_list" => execute_read_tool(client, options, "ls", args),
        "nokv_workbench_stat" => execute_read_tool(client, options, "stat", args),
        "nokv_workbench_read" => execute_read_tool(client, options, "read", args),
        "nokv_workbench_grep" => execute_read_tool(client, options, "grep", args),
        "nokv_workbench_commit" => commit_workbench(client, options, args),
        "nokv_workbench_snapshot" => snapshot_workbench(client, options, args),
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
    let rel_path = required_relative_path(args, "path")?;
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
            let rel_path = required_relative_path(args, "path")?;
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
    let result = nokv_agent::execute_agent_tool(client, read_tool, &Value::Object(forwarded))
        .map_err(|err| WorkbenchToolError::new(err.to_string()))?;
    Ok(json!({
        "workbench_id": id,
        "path": target,
        "result": result,
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
    let manifest_path = section_path(options, &id, "metadata", Some("run_manifest.json"));
    if client
        .metadata()
        .stat_path(&manifest_path)
        .map_err(client_error)?
        .is_none()
    {
        return Err(WorkbenchToolError::new(format!(
            "workbench {id} is not committed; missing {manifest_path}"
        )));
    }
    let path = workbench_path(options, &id);
    let snapshot = client
        .metadata()
        .snapshot_subtree_path(&path)
        .map_err(client_error)?;
    Ok(json!({
        "status": "success",
        "workbench_id": id,
        "path": path,
        "snapshot_id": snapshot.snapshot_id,
        "read_version": snapshot.read_version,
    }))
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

fn ensure_dir_path<O>(
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
    client
        .metadata()
        .mkdir(path, DEFAULT_MODE_DIR, options.uid, options.gid)
        .map_err(client_error)?;
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
                Some(raw) => Some(normalize_relative_path(raw, "path", true)?),
                None => None,
            };
            Ok(section_path(options, id, section, rel.as_deref()))
        }
    }
}

fn required_relative_path(args: &Value, field: &'static str) -> Result<String, WorkbenchToolError> {
    normalize_relative_path(required_string(args, field)?, field, false)
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

fn client_error(err: impl fmt::Display) -> WorkbenchToolError {
    WorkbenchToolError::new(err.to_string())
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
}
