/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Line-delimited JSON-RPC transport for the exact Workbench tool contract.
//!
//! This module deliberately knows nothing about metadata layout or object
//! providers. Both the custom CLI and MCP dispatch through the same validated
//! [`nokv_agent::WorkbenchToolHandler`].

use std::io::{self, BufRead, Write};

use nokv_agent::{execute_tool, tool_definitions, WorkbenchToolHandler};
use serde::Deserialize;
use serde_json::{json, Value};

const SUPPORTED_PROTOCOL_VERSIONS: [&str; 4] =
    ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

#[derive(Deserialize)]
struct JsonRpcRequest {
    jsonrpc: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present")]
    id: Option<Option<Value>>,
    method: Option<String>,
    params: Option<Value>,
}

#[derive(Deserialize)]
struct CallToolParams {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

fn deserialize_present<'de, D>(deserializer: D) -> Result<Option<Option<Value>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

pub fn serve(
    handler: &impl WorkbenchToolHandler,
    mut reader: impl BufRead,
    mut writer: impl Write,
) -> io::Result<()> {
    let mut line = String::new();
    while reader.read_line(&mut line)? != 0 {
        let request = line.trim();
        if !request.is_empty() {
            if let Some(response) = handle_line(handler, request) {
                serde_json::to_writer(&mut writer, &response)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
        line.clear();
    }
    Ok(())
}

fn handle_line(handler: &impl WorkbenchToolHandler, encoded: &str) -> Option<Value> {
    let raw = match serde_json::from_str::<Value>(encoded) {
        Ok(raw) => raw,
        Err(_) => return Some(error_response(None, -32700, "Parse error")),
    };
    if raw.is_array() {
        return Some(error_response(
            None,
            -32600,
            "Batch requests are not supported",
        ));
    }
    if !raw.is_object() {
        return Some(error_response(None, -32600, "Invalid Request"));
    }

    let request = match serde_json::from_value::<JsonRpcRequest>(raw) {
        Ok(request) => request,
        Err(_) => return Some(error_response(None, -32600, "Invalid Request")),
    };
    let id = request.id.clone().flatten();
    let notification = request.id.is_none();
    if request.jsonrpc.as_deref() != Some("2.0") {
        return response_unless_notification(
            notification,
            error_response(id, -32600, "Invalid Request: jsonrpc must be \"2.0\""),
        );
    }
    let Some(method) = request.method.as_deref() else {
        return response_unless_notification(
            notification,
            error_response(id, -32600, "Invalid Request: missing method"),
        );
    };

    let result = dispatch(handler, method, request.params);
    if notification {
        return None;
    }
    Some(match result {
        Ok(result) => success_response(id, result),
        Err((code, message)) => error_response(id, code, &message),
    })
}

fn response_unless_notification(notification: bool, response: Value) -> Option<Value> {
    (!notification).then_some(response)
}

fn dispatch(
    handler: &impl WorkbenchToolHandler,
    method: &str,
    params: Option<Value>,
) -> Result<Value, (i64, String)> {
    match method {
        "initialize" => Ok(initialize_result(params.as_ref())),
        "notifications/initialized" => Ok(json!({})),
        "ping" => Ok(json!({})),
        "tools/list" => {
            let tools = tool_definitions()
                .into_iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.parameters,
                    })
                })
                .collect::<Vec<_>>();
            Ok(json!({"tools": tools}))
        }
        "tools/call" => {
            let params =
                serde_json::from_value::<CallToolParams>(params.unwrap_or_else(|| json!({})))
                    .map_err(|error| (-32602, format!("Invalid params: {error}")))?;
            let arguments = params.arguments.unwrap_or_else(|| json!({}));
            match execute_tool(handler, &params.name, &arguments) {
                Ok(value) => Ok(tool_result(value, false)),
                Err(error) => Ok(tool_result(error.as_value(), true)),
            }
        }
        other => Err((-32601, format!("Method not found: {other}"))),
    }
}

fn initialize_result(params: Option<&Value>) -> Value {
    let requested = params
        .and_then(|params| params.get("protocolVersion"))
        .and_then(Value::as_str);
    let protocol_version = requested
        .filter(|version| SUPPORTED_PROTOCOL_VERSIONS.contains(version))
        .unwrap_or(SUPPORTED_PROTOCOL_VERSIONS[0]);
    json!({
        "protocolVersion": protocol_version,
        "capabilities": {"tools": {}},
        "serverInfo": {
            "name": "nokv-mcp",
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

fn tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value)
        .unwrap_or_else(|_| "{\"status\":\"error\",\"code\":\"SerializationFailure\"}".to_owned());
    let mut result = json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": value,
    });
    if is_error {
        result["isError"] = Value::Bool(true);
    }
    result
}

fn success_response(id: Option<Value>, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: Option<Value>, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    })
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use nokv_agent::AgentError;

    use super::*;

    fn handler(name: &str, arguments: &Value) -> Result<Value, AgentError> {
        Ok(json!({"status": "ok", "tool": name, "arguments": arguments}))
    }

    fn exchange(lines: &[&str]) -> Vec<Value> {
        let input = format!("{}\n", lines.join("\n"));
        let mut output = Vec::new();
        serve(
            &handler,
            BufReader::new(Cursor::new(input.into_bytes())),
            &mut output,
        )
        .unwrap();
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn initialize_negotiates_and_lists_exact_contract() {
        let responses = exchange(&[
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
        ]);
        assert_eq!(responses[0]["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(responses[0]["result"]["capabilities"], json!({"tools": {}}));
        assert_eq!(responses[0]["result"]["serverInfo"]["name"], "nokv-mcp");
        assert_eq!(
            responses[1]["result"]["tools"].as_array().unwrap().len(),
            nokv_agent::WORKBENCH_TOOL_COUNT
        );
        assert_eq!(
            responses[1]["result"]["tools"][0]["name"],
            "workbench_create"
        );
    }

    #[test]
    fn call_uses_validated_handler_and_structured_result() {
        let responses = exchange(&[
            r#"{"jsonrpc":"2.0","id":"call","method":"tools/call","params":{"name":"workbench_create","arguments":{"id":"run-1"}}}"#,
        ]);
        assert_eq!(responses[0]["result"]["structuredContent"]["status"], "ok");
        assert_eq!(
            responses[0]["result"]["structuredContent"]["tool"],
            "workbench_create"
        );
        let structured = &responses[0]["result"]["structuredContent"];
        assert_eq!(
            responses[0]["result"]["content"][0]["text"],
            serde_json::to_string_pretty(structured).unwrap()
        );
        assert!(responses[0]["result"].get("isError").is_none());
    }

    #[test]
    fn invalid_tool_arguments_are_tool_errors() {
        let responses = exchange(&[
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"workbench_create","arguments":{}}}"#,
        ]);
        assert_eq!(responses[0]["result"]["isError"], true);
        assert_eq!(
            responses[0]["result"]["structuredContent"]["code"],
            "InvalidArguments"
        );
    }

    #[test]
    fn notifications_do_not_emit_responses() {
        let responses = exchange(&[
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            r#"{"jsonrpc":"2.0","method":"initialized"}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#,
        ]);
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0]["id"], 1);
    }

    #[test]
    fn malformed_and_unknown_requests_return_json_rpc_errors() {
        let responses = exchange(&["{", r#"{"jsonrpc":"2.0","id":1,"method":"nope"}"#, r#"[]"#]);
        assert_eq!(responses[0]["error"]["code"], -32700);
        assert_eq!(responses[1]["error"]["code"], -32601);
        assert_eq!(responses[2]["error"]["code"], -32600);
    }
}
