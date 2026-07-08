use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use nokv_agent::event::{
    ingest_jsonl_reader, AgentEventError, AgentEventResult, AgentEventStore,
    CompletionAfterRequest, ErrorEventsRequest, EventRecord, HoltAgentEventStore,
    JsonlIngestOptions, LatestEventsRequest, NotificationBlockSnapshot,
    NotificationEventByIdRequest, NotificationEventsRequest, NotificationLifecycleRequest,
    NotificationNeighborDirection, NotificationNeighborRequest, NotificationSummaryEntry,
    RecentTimesRequest, SessionEventsRequest, SessionRowsRequest, ToolFacetRequest,
    ToolTraceRequest, TuiClearCompletionRequest, LINGTAI_SESSION_EVENT_TYPES,
};
use serde_json::json;

const DEFAULT_ERROR_EVENT_TYPES: &[&str] = &["aed_attempt", "aed_exhausted", "refresh_init_error"];

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(2);
    }
}

fn run() -> AgentEventResult<()> {
    let mut args = std::env::args().skip(1).collect::<VecDeque<_>>();
    let Some(scope) = args.pop_front() else {
        return usage();
    };
    if scope != "lingtai" {
        return usage();
    }
    let Some(command) = args.pop_front() else {
        return usage();
    };
    match command.as_str() {
        "ingest" => cmd_ingest(args),
        "coverage" => cmd_coverage(args),
        "latest" => cmd_latest(args),
        "session" => cmd_session(args),
        "session-rows" => cmd_session_rows(args),
        "recent" => cmd_recent(args),
        "molt-windows" => cmd_molt_windows(args),
        "errors" => cmd_errors(args),
        "completion-after" => cmd_completion_after(args),
        "clear-completion" => cmd_clear_completion(args),
        "notification-blocks" => cmd_notification_blocks(args),
        "notification-block-snapshots" => cmd_notification_block_snapshots(args),
        "notification-events" => cmd_notification_events(args),
        "notification-by-id" => cmd_notification_by_id(args),
        "notification-before" => {
            cmd_notification_neighbor(args, NotificationNeighborDirection::Before)
        }
        "notification-after" => {
            cmd_notification_neighbor(args, NotificationNeighborDirection::After)
        }
        "notifications" => cmd_notifications(args),
        "facets" => cmd_facets(args),
        "trace" => cmd_trace(args),
        _ => usage(),
    }
}

fn cmd_ingest(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let events_path = required_path(&mut args, "--events")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let source_file = option_value(&mut args, "--source-file")?
        .unwrap_or_else(|| events_path.display().to_string());
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    let file = File::open(&events_path)?;
    let file_size = file.metadata()?.len();
    let report = ingest_jsonl_reader(
        &store,
        JsonlIngestOptions {
            agent_id,
            source_file,
            file_size,
        },
        BufReader::new(file),
    )?;
    print_json(json!({"report": report}))
}

fn cmd_coverage(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let source_file = required_value(&mut args, "--source-file")?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "coverage": store.coverage(&agent_id, &source_file)?
    }))
}

fn cmd_latest(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_type = required_value(&mut args, "--type")?;
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(10);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.latest_events(LatestEventsRequest {
            agent_id,
            event_type,
            limit,
        })?
    }))
}

fn cmd_notification_blocks(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let events = latest_fixed_type(&mut args, "notification_pair_injected")?;
    let blocks = events
        .iter()
        .cloned()
        .map(NotificationSummaryEntry::from_event)
        .collect::<Vec<_>>();
    print_json(json!({
        "blocks": blocks,
        "events": events,
    }))
}

fn cmd_notification_block_snapshots(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let events = latest_fixed_type(&mut args, "notification_block_injected")?;
    let snapshots = events
        .iter()
        .cloned()
        .map(NotificationBlockSnapshot::from_event)
        .collect::<Vec<_>>();
    print_json(json!({
        "snapshots": snapshots,
        "events": events,
    }))
}

fn latest_fixed_type(
    args: &mut VecDeque<String>,
    event_type: &'static str,
) -> AgentEventResult<Vec<EventRecord>> {
    let store_path = required_path(args, "--store")?;
    let agent_id = option_value(args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let limit = option_value(args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(10);
    reject_extra(std::mem::take(args))?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    store.latest_events(LatestEventsRequest {
        agent_id,
        event_type: event_type.to_owned(),
        limit,
    })
}

fn cmd_session(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_types = option_value(&mut args, "--types")?
        .map(|value| parse_csv(&value))
        .unwrap_or_else(|| {
            LINGTAI_SESSION_EVENT_TYPES
                .iter()
                .map(|value| value.to_string())
                .collect()
        });
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.stream_session_events(SessionEventsRequest {
            agent_id,
            event_types,
            limit,
        })?
    }))
}

fn cmd_session_rows(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "rows": store.stream_session_rows(SessionRowsRequest {
            agent_id,
            limit,
        })?
    }))
}

fn cmd_recent(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_type = required_value(&mut args, "--type")?;
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(10);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "times": store.recent_times(RecentTimesRequest {
            agent_id,
            event_type,
            limit,
        })?
    }))
}

fn cmd_molt_windows(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "windows": store.molt_session_windows(&agent_id)?
    }))
}

fn cmd_errors(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_types = option_value(&mut args, "--types")?
        .map(|value| parse_csv(&value))
        .unwrap_or_else(|| {
            DEFAULT_ERROR_EVENT_TYPES
                .iter()
                .map(|value| value.to_string())
                .collect()
        });
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(50);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.error_events(ErrorEventsRequest {
            agent_id,
            event_types,
            limit,
        })?
    }))
}

fn cmd_completion_after(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_type =
        option_value(&mut args, "--type")?.unwrap_or_else(|| "clear_received".to_owned());
    let source_offset = parse_u64(
        "--source-offset",
        &required_value(&mut args, "--source-offset")?,
    )?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "completion": store.completion_after(CompletionAfterRequest {
            agent_id,
            event_type,
            source_offset,
        })?
    }))
}

fn cmd_clear_completion(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let source_offset = parse_u64(
        "--source-offset",
        &required_value(&mut args, "--source-offset")?,
    )?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "completion": store.tui_clear_completion(TuiClearCompletionRequest {
            agent_id,
            source_offset,
        })?
    }))
}

fn cmd_notification_events(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(50);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.notification_events(NotificationEventsRequest {
            agent_id,
            limit,
        })?
    }))
}

fn cmd_notification_by_id(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let event_id = parse_u64("--event-id", &required_value(&mut args, "--event-id")?)?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "event": store.notification_event_by_id(NotificationEventByIdRequest {
            agent_id,
            event_id,
        })?
    }))
}

fn cmd_notification_neighbor(
    mut args: VecDeque<String>,
    direction: NotificationNeighborDirection,
) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let pivot_event_id = parse_u64("--event-id", &required_value(&mut args, "--event-id")?)?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "event": store.notification_neighbor(NotificationNeighborRequest {
            agent_id,
            pivot_event_id,
            direction,
        })?
    }))
}

fn cmd_notifications(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let ref_id = option_value(&mut args, "--ref-id")?;
    let event_id = option_value(&mut args, "--event-id")?;
    let call_id = option_value(&mut args, "--call-id")?;
    let channel = option_value(&mut args, "--channel")?;
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(50);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.notification_lifecycle(NotificationLifecycleRequest {
            agent_id,
            ref_id,
            event_id,
            call_id,
            channel,
            limit,
        })?
    }))
}

fn cmd_facets(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let limit = option_value(&mut args, "--limit")?
        .map(|value| parse_usize("--limit", &value))
        .transpose()?
        .unwrap_or(20);
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "facets": store.tool_facets(ToolFacetRequest { agent_id, limit })?
    }))
}

fn cmd_trace(mut args: VecDeque<String>) -> AgentEventResult<()> {
    let store_path = required_path(&mut args, "--store")?;
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let tool_call_id = required_value(&mut args, "--tool-call-id")?;
    reject_extra(args)?;

    let store = HoltAgentEventStore::open_file(store_path)?;
    print_json(json!({
        "events": store.tool_trace(ToolTraceRequest {
            agent_id,
            tool_call_id,
        })?
    }))
}

fn required_path(args: &mut VecDeque<String>, flag: &str) -> AgentEventResult<PathBuf> {
    required_value(args, flag).map(PathBuf::from)
}

fn required_value(args: &mut VecDeque<String>, flag: &str) -> AgentEventResult<String> {
    option_value(args, flag)?
        .ok_or_else(|| AgentEventError::InvalidArgument(format!("missing required option {flag}")))
}

fn option_value(args: &mut VecDeque<String>, flag: &str) -> AgentEventResult<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };
    args.remove(index);
    args.remove(index)
        .map(Some)
        .ok_or_else(|| AgentEventError::InvalidArgument(format!("option {flag} requires a value")))
}

fn reject_extra(args: VecDeque<String>) -> AgentEventResult<()> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(AgentEventError::InvalidArgument(format!(
            "unexpected argument {}",
            args.front().unwrap()
        )))
    }
}

fn parse_usize(flag: &str, value: &str) -> AgentEventResult<usize> {
    value.parse::<usize>().map_err(|_| {
        AgentEventError::InvalidArgument(format!("option {flag} must be a positive integer"))
    })
}

fn parse_u64(flag: &str, value: &str) -> AgentEventResult<u64> {
    value.parse::<u64>().map_err(|_| {
        AgentEventError::InvalidArgument(format!("option {flag} must be an unsigned integer"))
    })
}

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn print_json(value: serde_json::Value) -> AgentEventResult<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn usage() -> AgentEventResult<()> {
    Err(AgentEventError::InvalidArgument(
        "usage: nokv-agent lingtai ingest|coverage|latest|session|session-rows|recent|molt-windows|errors|completion-after|clear-completion|notification-blocks|notification-block-snapshots|notification-events|notification-by-id|notification-before|notification-after|notifications|facets|trace".to_owned(),
    ))
}
