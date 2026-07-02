use std::io::{self, BufRead, Write};

use serde::Deserialize;
use serde_json::{json, Value};

use crate::{agent_tool_definitions, execute_agent_tool, AgentFs, AgentStore, AgentToolDefinition};

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    #[serde(default, deserialize_with = "deserialize_some")]
    id: Option<Option<Value>>,
    method: Option<String>,
    params: Option<Value>,
}

fn deserialize_some<'de, D>(deserializer: D) -> Result<Option<Option<Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct CallToolParams {
    name: String,
    arguments: Option<Value>,
}

pub trait McpToolSurface {
    fn tool_definitions(&self) -> Vec<AgentToolDefinition>;

    fn execute_tool(&self, name: &str, args: &Value) -> Result<Value, String>;
}

impl<S> McpToolSurface for AgentFs<S>
where
    S: AgentStore,
{
    fn tool_definitions(&self) -> Vec<AgentToolDefinition> {
        agent_tool_definitions()
    }

    fn execute_tool(&self, name: &str, args: &Value) -> Result<Value, String> {
        execute_agent_tool(self, name, args).map_err(|err| err.to_string())
    }
}

pub fn run_mcp<S>(fs: &AgentFs<S>) -> io::Result<()>
where
    S: AgentStore,
{
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_mcp_stream(fs, stdin.lock(), stdout.lock())
}

pub fn run_mcp_surface(surface: &impl McpToolSurface) -> io::Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_mcp_surface_stream(surface, stdin.lock(), stdout.lock())
}

pub fn run_mcp_stream<S>(
    fs: &AgentFs<S>,
    reader: impl BufRead,
    writer: impl Write,
) -> io::Result<()>
where
    S: AgentStore,
{
    run_mcp_surface_stream(fs, reader, writer)
}

pub fn run_mcp_surface_stream(
    surface: &impl McpToolSurface,
    mut reader: impl BufRead,
    mut writer: impl Write,
) -> io::Result<()> {
    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            let maybe_response = handle_line(surface, trimmed);
            if let Some(response) = maybe_response {
                serde_json::to_writer(&mut writer, &response)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
        line.clear();
    }
    Ok(())
}

fn handle_line(surface: &(impl McpToolSurface + ?Sized), trimmed: &str) -> Option<Value> {
    match serde_json::from_str::<Value>(trimmed) {
        Err(_) => Some(err_response(None, -32700, "Parse error")),
        Ok(raw) => {
            if raw.is_array() {
                return Some(err_response(
                    None,
                    -32600,
                    "Batch requests are not supported",
                ));
            }
            if !raw.is_object() {
                return Some(err_response(None, -32600, "Invalid Request"));
            }
            match serde_json::from_value::<JsonRpcRequest>(raw) {
                Err(_) => Some(err_response(None, -32600, "Invalid Request")),
                Ok(req) => handle_request(surface, req),
            }
        }
    }
}

fn handle_request(surface: &(impl McpToolSurface + ?Sized), req: JsonRpcRequest) -> Option<Value> {
    let id = req.id.clone().flatten();
    if req.jsonrpc.as_deref() != Some("2.0") {
        return Some(err_response(
            id,
            -32600,
            "Invalid Request: jsonrpc must be \"2.0\"",
        ));
    }
    let Some(method) = req.method else {
        return Some(err_response(id, -32600, "Invalid Request: missing method"));
    };

    let is_notification = req.id.is_none();
    let result = match method.as_str() {
        "initialize" => initialize_result(req.params.as_ref()),
        "initialized" | "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({
            "tools": surface
                .tool_definitions()
                .into_iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.parameters,
                    })
                })
                .collect::<Vec<_>>()
        })),
        "tools/call" => call_tool_result(surface, req.params),
        other => Err((-32601_i64, format!("Method not found: {other}"))),
    };

    if is_notification {
        None
    } else {
        Some(match result {
            Ok(val) => ok_response(id, val),
            Err((code, msg)) => err_response(id, code, &msg),
        })
    }
}

fn initialize_result(params: Option<&Value>) -> Result<Value, (i64, String)> {
    const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
        &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
    let requested = params
        .and_then(|p| p.get("protocolVersion"))
        .and_then(Value::as_str);
    let negotiated = match requested {
        Some(version) if SUPPORTED_PROTOCOL_VERSIONS.contains(&version) => version,
        _ => SUPPORTED_PROTOCOL_VERSIONS[0],
    };
    Ok(json!({
        "protocolVersion": negotiated,
        "capabilities": { "tools": {} },
        "serverInfo": { "name": "nokv-mcp", "version": "0.1.0" }
    }))
}

fn call_tool_result(
    surface: &(impl McpToolSurface + ?Sized),
    params: Option<Value>,
) -> Result<Value, (i64, String)> {
    let params = params.unwrap_or_else(|| json!({}));
    match serde_json::from_value::<CallToolParams>(params) {
        Err(e) => Err((-32602_i64, format!("Invalid params: {e}"))),
        Ok(call) => {
            let args = call.arguments.unwrap_or_else(|| json!({}));
            match surface.execute_tool(&call.name, &args) {
                Ok(val) => Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": serde_json::to_string_pretty(&val).unwrap_or_default()
                    }],
                    "structuredContent": val,
                })),
                Err(err) => Ok(json!({
                    "content": [{ "type": "text", "text": err.to_string() }],
                    "isError": true
                })),
            }
        }
    }
}

fn ok_response(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err_response(id: Option<Value>, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentId, HoltAgentStore};

    fn sample_fs() -> AgentFs<HoltAgentStore> {
        let fs = AgentFs::new(
            AgentId::new("mcp-test"),
            HoltAgentStore::open_memory().unwrap(),
        );
        fs.bootstrap().unwrap();
        fs
    }

    fn request(line: &str) -> Value {
        let fs = sample_fs();
        let reader = io::Cursor::new(format!("{line}\n"));
        let mut writer = Vec::new();
        run_mcp_stream(&fs, reader, &mut writer).unwrap();
        serde_json::from_slice(&writer).unwrap()
    }

    #[test]
    fn mcp_server_stdio_initializes_lists_and_calls_tools() {
        let fs = sample_fs();
        let reqs = [
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"test-client","version":"1.0"}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ls","arguments":{"path":"/"}}}"#,
        ]
        .join("\n")
            + "\n";
        let reader = io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(&fs, reader, &mut writer).unwrap();

        let output = String::from_utf8(writer).unwrap();
        let mut lines = output.lines();
        let resp1: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(resp1["id"], 1);
        assert_eq!(resp1["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(resp1["result"]["serverInfo"]["name"], "nokv-mcp");

        let resp2: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        let tools = resp2["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|t| t["name"] == "ls"));

        let resp3: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(resp3["id"], 3);
        assert!(resp3["result"]["content"][0]["text"].as_str().is_some());
    }

    #[test]
    fn tools_list_exposes_exactly_seven_tools() {
        let resp = request(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec!["ls", "stat", "catalog", "read", "aggregate", "find", "grep"]
        );
    }

    #[test]
    fn tools_list_passes_input_schema_unchanged() {
        let resp = request(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
        let tools = resp["result"]["tools"].as_array().unwrap();
        let ls_tool = tools.iter().find(|t| t["name"] == "ls").unwrap();
        let expected = agent_tool_definitions()
            .into_iter()
            .find(|t| t.name == "ls")
            .unwrap();
        assert_eq!(ls_tool["inputSchema"], expected.parameters);
    }

    #[test]
    fn tools_call_unknown_tool_returns_error_without_crashing() {
        let resp = request(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nonexistent","arguments":{}}}"#,
        );
        assert_eq!(resp["result"]["isError"], true);
    }

    #[test]
    fn tools_call_invalid_params_returns_jsonrpc_error() {
        let resp =
            request(r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"arguments":{}}}"#);
        assert_eq!(resp["error"]["code"], -32602);
    }

    #[test]
    fn unknown_method_returns_method_not_found() {
        let resp = request(r#"{"jsonrpc":"2.0","id":1,"method":"nonexistent/method"}"#);
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[test]
    fn rejects_malformed_requests() {
        assert_eq!(
            request(r#"{"id":1,"method":"tools/list"}"#)["error"]["code"],
            -32600
        );
        assert_eq!(
            request(r#"{"jsonrpc":"1.0","id":1,"method":"tools/list"}"#)["error"]["code"],
            -32600
        );
        assert_eq!(
            request(r#"[{"jsonrpc":"2.0","id":1,"method":"tools/list"}]"#)["error"]["code"],
            -32600
        );
    }

    #[test]
    fn invalid_json_returns_parse_error() {
        let fs = sample_fs();
        let mut writer = Vec::new();
        run_mcp_stream(&fs, io::Cursor::new("not valid json\n"), &mut writer).unwrap();
        let resp: Value = serde_json::from_slice(&writer).unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[test]
    fn null_id_gets_a_response_and_missing_id_is_notification() {
        let fs = sample_fs();
        let mut writer = Vec::new();
        run_mcp_stream(
            &fs,
            io::Cursor::new(r#"{"jsonrpc":"2.0","id":null,"method":"tools/list"}"#),
            &mut writer,
        )
        .unwrap();
        assert!(!writer.is_empty());

        writer.clear();
        run_mcp_stream(
            &fs,
            io::Cursor::new(r#"{"jsonrpc":"2.0","method":"initialized"}"#),
            &mut writer,
        )
        .unwrap();
        assert!(writer.is_empty());
    }

    #[test]
    fn initialize_negotiates_protocol_versions() {
        assert_eq!(
            request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"1999-01-01"}}"#
            )["result"]["protocolVersion"],
            "2025-11-25"
        );
        assert_eq!(
            request(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}"#
            )["result"]["protocolVersion"],
            "2025-06-18"
        );
        assert_eq!(
            request(r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)["result"]
                ["protocolVersion"],
            "2025-11-25"
        );
    }
}
