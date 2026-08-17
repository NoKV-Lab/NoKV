/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicUsize, Ordering};

use nokv_agent::{
    execute_generic_agent_tool, generic_agent_tool_definitions, AgentError,
    GenericAgentToolHandler, GENERIC_AGENT_CONTRACT_SCHEMA,
};
use serde_json::{json, Value};

#[test]
fn generic_agent_profile_preserves_the_exact_seven_tool_contract() {
    let tools = generic_agent_tool_definitions();
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["ls", "stat", "catalog", "read", "aggregate", "find", "grep"]
    );
    assert_eq!(
        GENERIC_AGENT_CONTRACT_SCHEMA,
        "nokv.agent.generic.mcp_input_schemas.v1"
    );

    let schema = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .expect("generic tool exists")
            .parameters
            .clone()
    };
    assert_eq!(
        schema("ls"),
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {"type": "string"},
                "cursor": {"type": ["string", "null"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100}
            },
            "additionalProperties": false
        })
    );
    assert_eq!(
        schema("read"),
        json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {"type": "string"},
                "format": {"type": "string", "enum": ["structured", "bytes"]},
                "cursor": {"type": ["string", "null"]},
                "offset": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 300}
            },
            "additionalProperties": false
        })
    );
    assert_eq!(
        schema("grep")["required"],
        json!(["path", "pattern", "recursive"])
    );
    assert_eq!(schema("grep")["properties"]["patterns"]["maxItems"], 16);
    assert_eq!(schema("find")["properties"]["limit"]["maximum"], 10);
    assert_eq!(schema("aggregate")["required"], json!(["path", "measures"]));
    for tool in tools {
        assert_eq!(tool.parameters["additionalProperties"], false);
        assert!(tool.parameters["properties"].get("id").is_none());
        assert!(tool.parameters["properties"].get("section").is_none());
    }
}

struct CountingHandler(AtomicUsize);

impl GenericAgentToolHandler for CountingHandler {
    fn execute(&self, _name: &str, _arguments: &Value) -> Result<Value, AgentError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"ok": true}))
    }
}

#[test]
fn generic_agent_schema_rejects_before_backend_dispatch() {
    let handler = CountingHandler(AtomicUsize::new(0));
    let error = execute_generic_agent_tool(&handler, "ls", &json!({"id": "wrong-shell"}))
        .expect_err("path is required and Workbench-shell fields are closed");
    assert_eq!(error.code, "InvalidArguments");
    assert_eq!(handler.0.load(Ordering::SeqCst), 0);
}
