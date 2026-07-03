use std::io::Cursor;
use std::process::Command;

use nokv_agent::event::{
    ingest_jsonl_reader, AgentEventStore, CompletionAfterRequest, ErrorEventsRequest,
    HoltAgentEventStore, JsonlIngestOptions, LatestEventsRequest, NotificationLifecycleRequest,
    NotificationNeighborDirection, NotificationNeighborRequest, RecentTimesRequest,
    SessionEventsRequest, SessionRowsRequest, ToolFacetRequest, ToolTraceRequest,
    TuiClearCompletionRequest,
};

fn ingest(store: &HoltAgentEventStore, jsonl: &str) -> nokv_agent::event::IngestReport {
    ingest_jsonl_reader(
        store,
        JsonlIngestOptions {
            agent_id: "agent-a".to_owned(),
            source_file: "logs/events.jsonl".to_owned(),
            file_size: jsonl.len() as u64,
        },
        Cursor::new(jsonl.as_bytes()),
    )
    .unwrap()
}

#[test]
fn ingest_replay_is_idempotent_and_keeps_coverage_monotonic() {
    let store = HoltAgentEventStore::open_memory().unwrap();
    let jsonl = concat!(
        r#"{"type":"tool_call","ts":1.0,"tool_name":"read","tool_call_id":"call-1","tool_args":{"file_path":"/tmp/a.md","offset":10,"limit":20}}"#,
        "\n",
        r#"{"type":"tool_result","ts":2.0,"tool_name":"read","tool_call_id":"call-1","status":"ok"}"#,
        "\n",
    );

    let first = ingest(&store, jsonl);
    assert_eq!(first.accepted, 2);
    assert_eq!(first.duplicates, 0);
    assert_eq!(first.coverage.row_count, 2);
    assert_eq!(first.coverage.min_offset, Some(0));
    assert!(first.coverage.max_offset.unwrap() > 0);

    let second = ingest(&store, jsonl);
    assert_eq!(second.accepted, 0);
    assert_eq!(second.duplicates, 2);

    let coverage = store
        .coverage("agent-a", "logs/events.jsonl")
        .unwrap()
        .unwrap();
    assert_eq!(coverage.row_count, 2);
    assert_eq!(coverage.file_size, jsonl.len() as u64);
}

#[test]
fn chunked_ingest_keeps_distinct_type_index_entries() {
    let store = HoltAgentEventStore::open_memory().unwrap();
    let mut jsonl = String::new();
    for index in 0..30_000 {
        jsonl.push_str(
            &serde_json::json!({
                "type": "tool_call",
                "ts": index as f64,
                "tool_name": "read",
                "tool_call_id": format!("call-{index}"),
            })
            .to_string(),
        );
        jsonl.push('\n');
    }

    let report = ingest(&store, &jsonl);
    assert_eq!(report.accepted, 30_000);

    let latest = store
        .latest_events(LatestEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "tool_call".to_owned(),
            limit: 3,
        })
        .unwrap();
    assert_eq!(latest.len(), 3);
    assert_eq!(
        latest
            .iter()
            .map(|record| record.projection.tool_call_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("call-29999"), Some("call-29998"), Some("call-29997")]
    );
    assert_eq!(
        store
            .latest_events(LatestEventsRequest {
                agent_id: "agent-a".to_owned(),
                event_type: "tool_call".to_owned(),
                limit: 40_000,
            })
            .unwrap()
            .len(),
        30_000
    );
    assert_eq!(
        store
            .stream_session_rows(SessionRowsRequest {
                agent_id: "agent-a".to_owned(),
                limit: None,
            })
            .unwrap()
            .len(),
        30_000
    );
}

#[test]
fn event_queries_match_lingtai_sqlite_shapes() {
    let store = HoltAgentEventStore::open_memory().unwrap();
    let jsonl = concat!(
        r#"{"type":"thinking","ts":0.5,"text":"hidden"}"#,
        "\n",
        r#"{"type":"tool_call","ts":1.0,"tool_name":"bash","tool_call_id":"call-1","tool_args":{"action":"run","command":"python3 script.py"}}"#,
        "\n",
        r#"{"type":"tool_result","ts":2.0,"tool_name":"bash","tool_call_id":"call-1","status":"ok"}"#,
        "\n",
        r#"{"type":"tool_call","ts":3.0,"tool_name":"read","tool_call_id":"call-2","tool_args":{"file_path":"/tmp/a.md","offset":10,"limit":20}}"#,
        "\n",
        r#"{"type":"notification_block_injected","ts":4.0,"call_id":"notify-call-1","channel":"work"}"#,
        "\n",
        r#"{"type":"system_notification_published","ts":5.0,"event_id":"event-1","ref_id":"ref-1"}"#,
        "\n",
        r#"{"type":"notification_event_dismiss","ts":6.0,"event_id":"event-1","ref_id":"ref-1","channel":"work"}"#,
        "\n",
        r#"{"type":"refresh_complete","ts":7.0}"#,
        "\n",
        r#"{"type":"psyche_molt","ts":8.0}"#,
        "\n",
        r#"{"type":"aed_attempt","ts":9.0,"error":"over window"}"#,
        "\n",
        r#"{"type":"clear_received","ts":10.0,"source":"tui"}"#,
        "\n",
    );
    ingest(&store, jsonl);

    let latest = store
        .latest_events(LatestEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "tool_call".to_owned(),
            limit: 2,
        })
        .unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[0].projection.tool_name.as_deref(), Some("read"));
    assert_eq!(
        latest[1].projection.command_head.as_deref(),
        Some("python3")
    );

    let session = store
        .stream_session_events(SessionEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_types: vec!["thinking".to_owned(), "tool_result".to_owned()],
            limit: None,
        })
        .unwrap();
    assert_eq!(
        session
            .iter()
            .map(|record| record.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["thinking", "tool_result"]
    );
    let session_rows = store
        .stream_session_rows(SessionRowsRequest {
            agent_id: "agent-a".to_owned(),
            limit: Some(3),
        })
        .unwrap();
    assert_eq!(
        session_rows
            .iter()
            .map(|row| row.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["thinking", "tool_call", "tool_result"]
    );
    assert_eq!(session_rows[0].fields_json["text"], "hidden");
    assert_eq!(session_rows[1].fields_json["tool_name"], "bash");

    let facets = store
        .tool_facets(ToolFacetRequest {
            agent_id: "agent-a".to_owned(),
            limit: 4,
        })
        .unwrap();
    assert_eq!(facets[0].tool_name, "bash");
    assert_eq!(facets[0].action.as_deref(), Some("run"));
    assert_eq!(facets[0].count, 1);

    let trace = store
        .tool_trace(ToolTraceRequest {
            agent_id: "agent-a".to_owned(),
            tool_call_id: "call-1".to_owned(),
        })
        .unwrap();
    assert_eq!(trace.len(), 2);
    assert_eq!(
        trace
            .iter()
            .map(|record| record.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["tool_call", "tool_result"]
    );

    let refresh_times = store
        .recent_times(RecentTimesRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "refresh_complete".to_owned(),
            limit: 10,
        })
        .unwrap();
    assert_eq!(refresh_times.len(), 1);
    assert_eq!(refresh_times[0].ts, 7.0);

    let molt_windows = store.molt_session_windows("agent-a").unwrap();
    assert!(molt_windows.ok);
    assert_eq!(molt_windows.current_since, Some(8.0));
    assert_eq!(molt_windows.last_since, None);
    assert_eq!(molt_windows.last_before, None);

    let error_events = store
        .error_events(ErrorEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_types: vec!["aed_attempt".to_owned(), "refresh_init_error".to_owned()],
            limit: 10,
        })
        .unwrap();
    assert_eq!(error_events.len(), 1);
    assert_eq!(
        error_events[0].projection.error.as_deref(),
        Some("over window")
    );

    let completion = store
        .completion_after(CompletionAfterRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "clear_received".to_owned(),
            source_offset: 0,
        })
        .unwrap();
    assert!(completion.found);
    assert_eq!(
        completion.event.unwrap().event_type.as_str(),
        "clear_received"
    );
    let clear_received = store
        .latest_events(LatestEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "clear_received".to_owned(),
            limit: 1,
        })
        .unwrap()
        .pop()
        .unwrap();
    let clear_completion = store
        .tui_clear_completion(TuiClearCompletionRequest {
            agent_id: "agent-a".to_owned(),
            source_offset: clear_received.source_offset,
        })
        .unwrap();
    assert!(clear_completion.found);
    let no_clear_completion = store
        .tui_clear_completion(TuiClearCompletionRequest {
            agent_id: "agent-a".to_owned(),
            source_offset: clear_received.source_offset + 1,
        })
        .unwrap();
    assert!(!no_clear_completion.found);

    let notification_by_ref = store
        .notification_lifecycle(NotificationLifecycleRequest {
            agent_id: "agent-a".to_owned(),
            ref_id: Some("ref-1".to_owned()),
            limit: 10,
            ..NotificationLifecycleRequest::default()
        })
        .unwrap();
    assert_eq!(
        notification_by_ref
            .iter()
            .map(|record| record.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "system_notification_published",
            "notification_event_dismiss"
        ]
    );

    let notification_by_call = store
        .notification_lifecycle(NotificationLifecycleRequest {
            agent_id: "agent-a".to_owned(),
            call_id: Some("notify-call-1".to_owned()),
            limit: 10,
            ..NotificationLifecycleRequest::default()
        })
        .unwrap();
    assert_eq!(notification_by_call.len(), 1);
    assert_eq!(
        notification_by_call[0].event_type.as_str(),
        "notification_block_injected"
    );

    let notification_events = store
        .notification_events(nokv_agent::event::NotificationEventsRequest {
            agent_id: "agent-a".to_owned(),
            limit: 10,
        })
        .unwrap();
    assert_eq!(
        notification_events
            .iter()
            .map(|record| record.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "notification_event_dismiss",
            "system_notification_published",
            "notification_block_injected"
        ]
    );
    let middle_id = notification_events[1].id;
    let by_id = store
        .notification_event_by_id(nokv_agent::event::NotificationEventByIdRequest {
            agent_id: "agent-a".to_owned(),
            event_id: middle_id,
        })
        .unwrap()
        .unwrap();
    assert_eq!(by_id.event_type, "system_notification_published");
    let before = store
        .notification_neighbor(NotificationNeighborRequest {
            agent_id: "agent-a".to_owned(),
            pivot_event_id: middle_id,
            direction: NotificationNeighborDirection::Before,
        })
        .unwrap()
        .unwrap();
    assert_eq!(before.event_type, "notification_block_injected");
    let after = store
        .notification_neighbor(NotificationNeighborRequest {
            agent_id: "agent-a".to_owned(),
            pivot_event_id: middle_id,
            direction: NotificationNeighborDirection::After,
        })
        .unwrap()
        .unwrap();
    assert_eq!(after.event_type, "notification_event_dismiss");
}

#[test]
fn partial_trailing_line_is_not_indexed() {
    let store = HoltAgentEventStore::open_memory().unwrap();
    let jsonl = concat!(
        r#"{"type":"tool_call","ts":1.0,"tool_name":"read","tool_call_id":"call-1"}"#,
        "\n",
        r#"{"type":"tool_call","ts":2.0,"tool_name":"bash""#,
    );

    let report = ingest(&store, jsonl);
    assert_eq!(report.accepted, 1);
    assert_eq!(report.partial_lines, 1);
    assert_eq!(report.parse_errors, 0);
    assert_eq!(
        store
            .latest_events(LatestEventsRequest {
                agent_id: "agent-a".to_owned(),
                event_type: "tool_call".to_owned(),
                limit: 10,
            })
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn large_fields_json_is_compacted_for_holt_value_limit() {
    let store = HoltAgentEventStore::open_memory().unwrap();
    let large_text = "x".repeat(70 * 1024);
    let jsonl = format!(
        "{}\n",
        serde_json::json!({
            "type": "tool_result",
            "ts": 1.0,
            "tool_name": "bash",
            "tool_call_id": "call-large",
            "result": large_text,
        })
    );

    let report = ingest(&store, &jsonl);
    assert_eq!(report.accepted, 1);

    let events = store
        .latest_events(LatestEventsRequest {
            agent_id: "agent-a".to_owned(),
            event_type: "tool_result".to_owned(),
            limit: 1,
        })
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].projection.tool_name.as_deref(), Some("bash"));
    assert_eq!(events[0].fields_json["_nokv_compacted"], true);
    assert_eq!(events[0].fields_json["keys"][0], "result");
}

#[test]
fn file_backed_holt_store_reopens_indexed_events() {
    let dir = tempfile::tempdir().unwrap();
    let jsonl = concat!(
        r#"{"type":"tool_call","ts":1.0,"tool_name":"read","tool_call_id":"call-1","tool_args":{"file_path":"/tmp/a.md"}}"#,
        "\n",
    );
    {
        let store = HoltAgentEventStore::open_file(dir.path()).unwrap();
        let report = ingest(&store, jsonl);
        assert_eq!(report.accepted, 1);
    }
    {
        let store = HoltAgentEventStore::open_file(dir.path()).unwrap();
        let coverage = store
            .coverage("agent-a", "logs/events.jsonl")
            .unwrap()
            .unwrap();
        assert_eq!(coverage.row_count, 1);
        let latest = store
            .latest_events(LatestEventsRequest {
                agent_id: "agent-a".to_owned(),
                event_type: "tool_call".to_owned(),
                limit: 1,
            })
            .unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].projection.tool_name.as_deref(), Some("read"));
    }
}

#[test]
fn nokv_agent_lingtai_cli_round_trips_json() {
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join("agent-index");
    let events_path = dir.path().join("events.jsonl");
    std::fs::write(
        &events_path,
        concat!(
            r#"{"type":"tool_call","ts":1.0,"tool_name":"read","tool_call_id":"call-1","tool_args":{"file_path":"/tmp/a.md"}}"#,
            "\n",
            r#"{"type":"notification_pair_injected","ts":2.0,"call_id":"notify-call-1","summary":"note"}"#,
            "\n",
            r#"{"type":"notification_block_injected","ts":3.0,"call_id":"notify-call-1","summary":"block"}"#,
            "\n",
            r#"{"type":"clear_received","ts":4.0,"source":"tui"}"#,
            "\n",
        ),
    )
    .unwrap();

    let bin = env!("CARGO_BIN_EXE_nokv-agent");
    let ingest = Command::new(bin)
        .args([
            "lingtai",
            "ingest",
            "--store",
            store_dir.to_str().unwrap(),
            "--events",
            events_path.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--source-file",
            "logs/events.jsonl",
        ])
        .output()
        .unwrap();
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );
    let ingest_json: serde_json::Value = serde_json::from_slice(&ingest.stdout).unwrap();
    assert_eq!(ingest_json["report"]["accepted"], 4);

    let coverage = Command::new(bin)
        .args([
            "lingtai",
            "coverage",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--source-file",
            "logs/events.jsonl",
        ])
        .output()
        .unwrap();
    assert!(
        coverage.status.success(),
        "{}",
        String::from_utf8_lossy(&coverage.stderr)
    );
    let coverage_json: serde_json::Value = serde_json::from_slice(&coverage.stdout).unwrap();
    assert_eq!(coverage_json["coverage"]["row_count"], 4);

    let latest = Command::new(bin)
        .args([
            "lingtai",
            "latest",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--type",
            "tool_call",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        latest.status.success(),
        "{}",
        String::from_utf8_lossy(&latest.stderr)
    );
    let latest_json: serde_json::Value = serde_json::from_slice(&latest.stdout).unwrap();
    assert_eq!(latest_json["events"][0]["projection"]["tool_name"], "read");

    let session_rows = Command::new(bin)
        .args([
            "lingtai",
            "session-rows",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        session_rows.status.success(),
        "{}",
        String::from_utf8_lossy(&session_rows.stderr)
    );
    let session_rows_json: serde_json::Value =
        serde_json::from_slice(&session_rows.stdout).unwrap();
    assert_eq!(session_rows_json["rows"][0]["event_type"], "tool_call");
    assert_eq!(
        session_rows_json["rows"][0]["fields_json"]["tool_name"],
        "read"
    );

    let recent = Command::new(bin)
        .args([
            "lingtai",
            "recent",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--type",
            "tool_call",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        recent.status.success(),
        "{}",
        String::from_utf8_lossy(&recent.stderr)
    );
    let recent_json: serde_json::Value = serde_json::from_slice(&recent.stdout).unwrap();
    assert_eq!(recent_json["times"][0]["ts"], 1.0);

    let notification_blocks = Command::new(bin)
        .args([
            "lingtai",
            "notification-blocks",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        notification_blocks.status.success(),
        "{}",
        String::from_utf8_lossy(&notification_blocks.stderr)
    );
    let notification_blocks_json: serde_json::Value =
        serde_json::from_slice(&notification_blocks.stdout).unwrap();
    assert_eq!(
        notification_blocks_json["events"][0]["event_type"],
        "notification_pair_injected"
    );

    let notification_block_snapshots = Command::new(bin)
        .args([
            "lingtai",
            "notification-block-snapshots",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        notification_block_snapshots.status.success(),
        "{}",
        String::from_utf8_lossy(&notification_block_snapshots.stderr)
    );
    let notification_block_snapshots_json: serde_json::Value =
        serde_json::from_slice(&notification_block_snapshots.stdout).unwrap();
    assert_eq!(
        notification_block_snapshots_json["events"][0]["event_type"],
        "notification_block_injected"
    );

    let notification_events = Command::new(bin)
        .args([
            "lingtai",
            "notification-events",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--limit",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        notification_events.status.success(),
        "{}",
        String::from_utf8_lossy(&notification_events.stderr)
    );
    let notification_events_json: serde_json::Value =
        serde_json::from_slice(&notification_events.stdout).unwrap();
    assert_eq!(
        notification_events_json["events"][0]["event_type"],
        "notification_block_injected"
    );
    let notification_id = notification_events_json["events"][0]["id"]
        .as_u64()
        .unwrap()
        .to_string();

    let notification_by_id = Command::new(bin)
        .args([
            "lingtai",
            "notification-by-id",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--event-id",
            &notification_id,
        ])
        .output()
        .unwrap();
    assert!(
        notification_by_id.status.success(),
        "{}",
        String::from_utf8_lossy(&notification_by_id.stderr)
    );
    let notification_by_id_json: serde_json::Value =
        serde_json::from_slice(&notification_by_id.stdout).unwrap();
    assert_eq!(
        notification_by_id_json["event"]["event_type"],
        "notification_block_injected"
    );

    let clear_completion = Command::new(bin)
        .args([
            "lingtai",
            "clear-completion",
            "--store",
            store_dir.to_str().unwrap(),
            "--agent-id",
            "agent-a",
            "--source-offset",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        clear_completion.status.success(),
        "{}",
        String::from_utf8_lossy(&clear_completion.stderr)
    );
    let clear_completion_json: serde_json::Value =
        serde_json::from_slice(&clear_completion.stdout).unwrap();
    assert_eq!(clear_completion_json["completion"]["found"], true);
}
