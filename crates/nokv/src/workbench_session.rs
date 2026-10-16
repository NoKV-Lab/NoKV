/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Persistent native CLI session: sequential Workbench tool calls over one
//! admitted client, one JSON request and one JSON response per line.
//!
//! A line names either one of the 18 Workbench `tool`s or the session
//! `command` `materialize`, the session form of `nokv materialize` that
//! returns one whole artifact inline instead of writing a local file. The
//! session only removes per-call process, routing, and admission setup.
//! Every call keeps its own publication, durability, and retry boundary; the
//! session is not a transaction, and a request `id` only correlates the
//! response line.

use std::io::{self, BufRead, Read, Write};

use nokv_agent::{execute_tool, AgentError, WorkbenchToolHandler};
use serde::Deserialize;
use serde_json::{json, Value};

/// Largest accepted request line, including its newline.
const MAX_REQUEST_LINE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_REQUEST_ID_BYTES: usize = 128;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRequest {
    id: String,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    command: Option<String>,
    arguments: Value,
}

/// Whole-artifact transfer that the CLI offers beside the Workbench tools.
pub trait SessionCommands {
    /// Return one live artifact's complete bytes with its generation.
    fn materialize(&self, arguments: &Value) -> Result<Value, AgentError>;
}

/// Serve requests until end of input. A malformed line is answered with
/// `id: null`; an oversized line ends the session before dispatch.
pub fn serve(
    handler: &impl WorkbenchToolHandler,
    commands: &impl SessionCommands,
    reader: impl BufRead,
    writer: impl Write,
) -> io::Result<()> {
    serve_bounded(handler, commands, reader, writer, MAX_REQUEST_LINE_BYTES)
}

fn dispatch(
    handler: &impl WorkbenchToolHandler,
    commands: &impl SessionCommands,
    request: &SessionRequest,
) -> Result<Value, AgentError> {
    match (&request.tool, request.command.as_deref()) {
        (Some(tool), None) => execute_tool(handler, tool, &request.arguments),
        (None, Some("materialize")) => commands.materialize(&request.arguments),
        _ => Err(AgentError::invalid_arguments(
            "a request names exactly one Workbench tool or the materialize command",
        )),
    }
}

fn serve_bounded(
    handler: &impl WorkbenchToolHandler,
    commands: &impl SessionCommands,
    mut reader: impl BufRead,
    mut writer: impl Write,
    maximum: u64,
) -> io::Result<()> {
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader
            .by_ref()
            .take(maximum + 1)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(());
        }
        if read as u64 > maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Workbench session request line exceeds its limit",
            ));
        }
        let response = match serde_json::from_slice::<SessionRequest>(&line) {
            Ok(request) if !request.id.is_empty() && request.id.len() <= MAX_REQUEST_ID_BYTES => {
                match dispatch(handler, commands, &request) {
                    Ok(result) => json!({"id": request.id, "result": result}),
                    Err(error) => json!({"id": request.id, "error": error.as_value()}),
                }
            }
            _ => json!({
                "id": null,
                "error": AgentError::invalid_arguments(
                    "expected {id, tool or command, arguments} with a 1-128 byte id; unknown fields are rejected"
                )
                .as_value(),
            }),
        };
        serde_json::to_writer(&mut writer, &response)?;
        writer.write_all(b"\n")?;
        // The caller blocks on this line; it must not stay buffered.
        writer.flush()?;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct EchoMaterialize;

    impl SessionCommands for EchoMaterialize {
        fn materialize(&self, arguments: &Value) -> Result<Value, AgentError> {
            Ok(json!({"materialized": arguments["path"]}))
        }
    }

    fn run(handler: &impl WorkbenchToolHandler, input: &str) -> Vec<Value> {
        let mut output = Vec::new();
        serve(handler, &EchoMaterialize, input.as_bytes(), &mut output).unwrap();
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn each_line_gets_one_correlated_response_from_one_client() {
        let calls = AtomicUsize::new(0);
        let handler = |name: &str, arguments: &Value| -> Result<Value, AgentError> {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok(json!({"tool": name, "workbench": arguments["id"]}))
        };
        let lines = run(
            &handler,
            concat!(
                "{\"id\":\"a\",\"tool\":\"workbench_create\",\"arguments\":{\"id\":\"run-1\"}}\n",
                "{\"id\":\"b\",\"tool\":\"no_such_tool\",\"arguments\":{}}\n",
                "{\"id\":\"c\",\"tool\":\"workbench_create\",\"arguments\":{\"id\":\"run-2\"}}\n",
            ),
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["id"], "a");
        assert_eq!(lines[0]["result"]["workbench"], "run-1");
        assert_eq!(lines[1]["id"], "b");
        assert!(lines[1]["error"].is_object());
        assert_eq!(lines[2]["result"]["workbench"], "run-2");
    }

    #[test]
    fn materialize_is_a_session_command_not_a_tool() {
        let handler = |_: &str, _: &Value| -> Result<Value, AgentError> {
            panic!("materialize must not dispatch a Workbench tool")
        };
        let lines = run(
            &handler,
            concat!(
                "{\"id\":\"m\",\"command\":\"materialize\",\"arguments\":{\"path\":\"a.txt\"}}\n",
                "{\"id\":\"n\",\"command\":\"unknown\",\"arguments\":{}}\n",
                "{\"id\":\"o\",\"tool\":\"workbench_read\",\"command\":\"materialize\",\"arguments\":{}}\n",
            ),
        );
        assert_eq!(lines[0]["result"]["materialized"], "a.txt");
        assert!(lines[1]["error"].is_object());
        assert!(lines[2]["error"].is_object());
    }

    #[test]
    fn malformed_lines_are_answered_without_dispatch() {
        let handler = |_: &str, _: &Value| -> Result<Value, AgentError> {
            panic!("a malformed line must not dispatch")
        };
        let lines = run(
            &handler,
            concat!(
                "not json\n",
                "{\"id\":\"\",\"tool\":\"workbench_create\",\"arguments\":{}}\n",
                "{\"id\":\"c\",\"tool\":\"workbench_create\",\"arguments\":{},\"extra\":1}\n",
            ),
        );
        assert_eq!(lines.len(), 3);
        assert!(lines.iter().all(|line| line["id"].is_null()));
    }

    #[test]
    fn an_oversized_line_ends_the_session_before_dispatch() {
        let handler = |_: &str, _: &Value| -> Result<Value, AgentError> {
            panic!("an oversized line must not dispatch")
        };
        let mut output = Vec::new();
        let error = serve_bounded(
            &handler,
            &EchoMaterialize,
            "{\"id\":\"x\"}\n".as_bytes(),
            &mut output,
            4,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(output.is_empty());
    }
}
