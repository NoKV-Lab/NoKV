//! Parity gate between the embedded workbench mirror and the DFS-backed
//! workbench MCP server.
//!
//! The DFS implementation lives in a bin crate
//! (`crates/nokv/src/bin/nokv/workbench_mcp.rs`) and cannot be imported here,
//! so its tool schemas and response key sets are pinned as golden constants.
//! When either side changes its tool surface, update BOTH the other side and
//! these constants in the same commit.

use serde_json::{json, Value};

use nokv_agent::{
    AgentFs, AgentId, HoltAgentStore, McpToolSurface, WorkbenchMcpOptions, WorkbenchMcpSurface,
    DEFAULT_WORKBENCH_MAX_BYTES, DEFAULT_WORKBENCH_ROOT,
};

fn fs() -> (tempfile::TempDir, AgentFs<HoltAgentStore>) {
    let dir = tempfile::tempdir().unwrap();
    let fs = AgentFs::new(
        AgentId::new("parity-test"),
        HoltAgentStore::open(dir.path().join("store")).unwrap(),
    );
    fs.bootstrap().unwrap();
    (dir, fs)
}

fn options() -> WorkbenchMcpOptions {
    WorkbenchMcpOptions {
        root: DEFAULT_WORKBENCH_ROOT.to_owned(),
        max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
    }
}

/// Golden copy of `tool_definitions()` from
/// `crates/nokv/src/bin/nokv/workbench_mcp.rs` (names and inputSchema only;
/// tool descriptions intentionally differ per backend).
fn dfs_tool_schemas() -> Vec<(&'static str, Value)> {
    let sections = json!(["input", "scripts", "outputs", "logs", "metadata"]);
    let nullable_section = json!({
        "type": ["string", "null"],
        "enum": ["input", "scripts", "outputs", "logs", "metadata", null]
    });
    vec![
        (
            "workbench_create",
            json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string", "description": "Workbench id, e.g. spedas-task-001."}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_put_file",
            json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": sections},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "text": {"type": "string"},
                    "base64": {"type": "string"},
                    "content_type": {"type": "string"},
                    "replace": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_list",
            json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": nullable_section,
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_stat",
            json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"},
                    "section": nullable_section,
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_read",
            json!({
                "type": "object",
                "required": ["id", "section", "path"],
                "properties": {
                    "id": {"type": "string"},
                    "section": {"type": "string", "enum": sections},
                    "path": {"type": "string", "description": "Path relative to section. Do not prefix it with the section name."},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_grep",
            json!({
                "type": "object",
                "required": ["id", "pattern", "recursive"],
                "properties": {
                    "id": {"type": "string"},
                    "section": nullable_section,
                    "path": {"type": ["string", "null"], "description": "Optional path relative to section. Do not prefix it with the section name."},
                    "pattern": {"type": "string"},
                    "recursive": {"type": "boolean"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_find",
            json!({
                "type": "object",
                "properties": {
                    "committed": {"type": ["boolean", "null"], "description": "Filter by completion marker. Null or omitted returns all workbenches."},
                    "manifest_pattern": {"type": ["string", "null"], "description": "Case-insensitive literal substring filter over metadata/run_manifest.json."},
                    "include_manifest": {"type": "boolean", "description": "Include full run_manifest.json envelopes. Defaults false."},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_commit",
            json!({
                "type": "object",
                "required": ["id", "manifest"],
                "properties": {
                    "id": {"type": "string"},
                    "manifest": {"type": "object"},
                    "replace": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        ),
        (
            "workbench_snapshot",
            json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "string"}
                },
                "additionalProperties": false
            }),
        ),
    ]
}

#[test]
fn mirror_tool_schemas_match_dfs_golden() {
    let (_dir, fs) = fs();
    let surface = WorkbenchMcpSurface::new(&fs, options());
    let actual = surface.tool_definitions();
    let expected = dfs_tool_schemas();
    assert_eq!(
        actual.iter().map(|tool| tool.name).collect::<Vec<_>>(),
        expected.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
    );
    for (tool, (name, parameters)) in actual.iter().zip(&expected) {
        assert_eq!(
            &tool.parameters, parameters,
            "inputSchema of {name} diverged from the DFS golden"
        );
    }
}

fn keys(value: &Value) -> Vec<&str> {
    let mut keys = value
        .as_object()
        .unwrap_or_else(|| panic!("expected object, got {value}"))
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

/// Asserts the mirror response carries exactly the DFS key set plus the
/// whitelisted mirror-only extras.
fn assert_key_parity(tool: &str, response: &Value, dfs_keys: &[&str], extras: &[&str]) {
    let mut expected = dfs_keys.iter().chain(extras).copied().collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(
        keys(response),
        expected,
        "{tool} response keys diverged from the DFS golden"
    );
}

#[test]
fn mirror_response_key_sets_match_dfs_golden() {
    let (_dir, fs) = fs();
    let surface = WorkbenchMcpSurface::new(&fs, options());
    let call = |name: &str, args: Value| surface.execute_tool(name, &args).unwrap();

    // DFS key sets transcribed from the success responses in
    // crates/nokv/src/bin/nokv/workbench_mcp.rs.
    let create = call("workbench_create", json!({"id": "wb-1"}));
    assert_key_parity(
        "workbench_create",
        &create,
        &["status", "workbench_id", "path", "sections"],
        &["backend"],
    );

    let put = call(
        "workbench_put_file",
        json!({"id": "wb-1", "section": "input", "path": "data.json", "text": "{\"event\":\"flare\"}"}),
    );
    assert_key_parity(
        "workbench_put_file",
        &put,
        &[
            "status",
            "workbench_id",
            "section",
            "relative_path",
            "path",
            "size_bytes",
            "inode",
            "generation",
            "digest_uri",
            "content_type",
            "replace",
        ],
        &["backend"],
    );
    assert_eq!(put["inode"], Value::Null);
    assert_eq!(put["generation"], Value::Null);

    let list = call("workbench_list", json!({"id": "wb-1"}));
    assert_key_parity(
        "workbench_list",
        &list,
        &[
            "status",
            "workbench_id",
            "workbench_path",
            "section",
            "relative_path",
            "path",
            "entry_count",
            "entries",
            "next_cursor",
            "truncated",
        ],
        &[],
    );
    assert_key_parity(
        "workbench_list entry",
        &list["entries"][0],
        &[
            "name",
            "path",
            "section",
            "relative_path",
            "kind",
            "size_bytes",
            "entry_count",
        ],
        &[],
    );

    let stat = call(
        "workbench_stat",
        json!({"id": "wb-1", "section": "input", "path": "data.json"}),
    );
    assert_key_parity(
        "workbench_stat",
        &stat,
        &[
            "status",
            "workbench_id",
            "workbench_path",
            "section",
            "relative_path",
            "path",
            "card",
        ],
        &[],
    );
    assert_key_parity(
        "workbench_stat card",
        &stat["card"],
        &[
            "name",
            "path",
            "section",
            "relative_path",
            "kind",
            "size_bytes",
            "entry_count",
            "record_count",
            "inode",
            "generation",
            "content_type",
            "digest_uri",
            "producer",
            "manifest_id",
        ],
        &[],
    );
    assert_eq!(stat["card"]["inode"], Value::Null);
    assert_eq!(stat["card"]["generation"], Value::Null);

    let read = call(
        "workbench_read",
        json!({"id": "wb-1", "section": "input", "path": "data.json"}),
    );
    assert_key_parity(
        "workbench_read",
        &read,
        &[
            "status",
            "workbench_id",
            "workbench_path",
            "section",
            "relative_path",
            "path",
            "total_size_bytes",
            "format",
            "record_type",
            "record_count",
            "cursor",
            "next_cursor",
            "truncated",
            "items",
            "bytes",
        ],
        &[],
    );

    let grep = call(
        "workbench_grep",
        json!({"id": "wb-1", "pattern": "flare", "recursive": true}),
    );
    assert_key_parity(
        "workbench_grep",
        &grep,
        &[
            "status",
            "workbench_id",
            "workbench_path",
            "section",
            "relative_path",
            "path",
            "pattern",
            "recursive",
            "matches",
            "files_scanned",
            "next_cursor",
            "truncated",
        ],
        &[],
    );
    assert_key_parity(
        "workbench_grep match",
        &grep["matches"][0],
        &["path", "section", "relative_path", "line_number", "snippet"],
        &[],
    );

    let commit = call(
        "workbench_commit",
        json!({"id": "wb-1", "manifest": {"task": "flare"}}),
    );
    assert_key_parity(
        "workbench_commit",
        &commit,
        &[
            "status",
            "workbench_id",
            "path",
            "size_bytes",
            "inode",
            "generation",
            "digest_uri",
            "replace",
        ],
        &["backend"],
    );
    assert_eq!(commit["inode"], Value::Null);
    assert_eq!(commit["generation"], Value::Null);

    let find = call("workbench_find", json!({}));
    assert_key_parity(
        "workbench_find",
        &find,
        &[
            "status",
            "path",
            "matches",
            "match_count",
            "entry_count",
            "next_cursor",
            "truncated",
        ],
        &[],
    );
    assert_key_parity(
        "workbench_find match",
        &find["matches"][0],
        &[
            "workbench_id",
            "path",
            "committed",
            "manifest_path",
            "manifest_size_bytes",
            "manifest_generation",
            "manifest_digest_uri",
            "manifest_summary",
            "manifest",
        ],
        &[],
    );
    assert_eq!(find["matches"][0]["manifest_generation"], Value::Null);

    let snapshot = call("workbench_snapshot", json!({"id": "wb-1"}));
    assert_key_parity(
        "workbench_snapshot",
        &snapshot,
        &[
            "status",
            "workbench_id",
            "path",
            "snapshot_id",
            "read_version",
        ],
        &[
            "backend",
            "snapshot_kind",
            "snapshot_path",
            "snapshot_digest",
        ],
    );
    assert_eq!(snapshot["read_version"], Value::Null);
}
