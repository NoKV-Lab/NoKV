use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use nokv_agent::event::{
    ingest_jsonl_reader, AgentEventStore, ErrorEventsRequest, EventRecord, HoltAgentEventStore,
    JsonlIngestOptions, LatestEventsRequest, NotificationEventByIdRequest,
    NotificationEventsRequest, NotificationNeighborDirection, NotificationNeighborRequest,
    RecentTimesRequest, SessionEventRow, SessionEventsRequest, SessionRowsRequest, ToolFacet,
    ToolFacetRequest, TuiClearCompletionRequest, LINGTAI_SESSION_EVENT_TYPES,
};
use rusqlite::{params_from_iter, Connection};
use serde::Serialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

const ERROR_EVENT_TYPES: &[&str] = &["aed_attempt", "aed_exhausted", "refresh_init_error"];
const MAX_INLINE_FIELDS_JSON_BYTES: usize = 32 * 1024;

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
struct Options {
    events_jsonl: PathBuf,
    sqlite: PathBuf,
    projected_sqlite: Option<PathBuf>,
    rebuilt_sqlite: Option<PathBuf>,
    agent_index: PathBuf,
    source_file: String,
    agent_id: String,
    iterations: usize,
    reset: bool,
}

#[derive(Clone, Debug, Serialize)]
struct QueryDigest {
    rows: usize,
    fingerprint: String,
}

#[derive(Clone, Debug, Serialize)]
struct QueryStats {
    rows: usize,
    p50_ms: f64,
    p95_ms: f64,
    avg_ms: f64,
    fingerprint: String,
}

#[derive(Clone, Debug, Serialize)]
struct QueryComparison {
    rows_match: bool,
    fingerprint_match: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct NotificationPivots {
    latest: Option<u64>,
    earliest: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct ProjectedSqliteStats {
    path: PathBuf,
    bytes: u64,
    build_ms: f64,
    sqlite_version: String,
    projected_index_sizes: Vec<SqliteIndexSize>,
    queries: BTreeMap<String, QueryStats>,
}

#[derive(Clone, Debug, Serialize)]
struct RebuiltSqliteStats {
    path: PathBuf,
    bytes: u64,
    build_ms: f64,
    sqlite_version: String,
    event_rows: u64,
    projected_index_sizes: Vec<SqliteIndexSize>,
    queries: BTreeMap<String, QueryStats>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteIndexSize {
    name: String,
    bytes: u64,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(2);
    }
}

fn run() -> BenchResult<()> {
    let options = parse_args()?;
    if options.reset && options.agent_index.exists() {
        std::fs::remove_dir_all(&options.agent_index)?;
    }

    let sqlite =
        Connection::open_with_flags(&options.sqlite, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    sqlite.execute_batch("pragma query_only=on")?;

    let ingest_started = Instant::now();
    let store = HoltAgentEventStore::open_file(&options.agent_index)?;
    let events_file = File::open(&options.events_jsonl)?;
    let file_size = events_file.metadata()?.len();
    let ingest_report = ingest_jsonl_reader(
        &store,
        JsonlIngestOptions {
            agent_id: options.agent_id.clone(),
            source_file: options.source_file.clone(),
            file_size,
        },
        BufReader::new(events_file),
    )?;
    let ingest_ms = elapsed_ms(ingest_started);

    let sqlite_queries = sqlite_queries(&sqlite, options.iterations)?;
    let projected_sqlite = options
        .projected_sqlite
        .as_ref()
        .map(|path| projected_sqlite_baseline(&options.sqlite, path, options.iterations))
        .transpose()?;
    let rebuilt_sqlite = options
        .rebuilt_sqlite
        .as_ref()
        .map(|path| rebuilt_sqlite_baseline(&options, path, options.iterations))
        .transpose()?;
    let agent_queries = agent_queries(&store, &options, options.iterations)?;
    let comparisons = compare_queries(&sqlite_queries, &agent_queries);
    let projected_comparisons = projected_sqlite
        .as_ref()
        .map(|projected| compare_queries(&projected.queries, &agent_queries));
    let rebuilt_comparisons = rebuilt_sqlite
        .as_ref()
        .map(|rebuilt| compare_queries(&rebuilt.queries, &agent_queries));

    let output = json!({
        "events_jsonl": {
            "path": options.events_jsonl,
            "bytes": file_size,
        },
        "sqlite_current_v1": {
            "path": options.sqlite,
            "bytes": options.sqlite.metadata()?.len(),
            "sqlite_version": sqlite.query_row("select sqlite_version()", [], |row| row.get::<_, String>(0))?,
            "queries": sqlite_queries,
        },
        "sqlite_projected_v1": projected_sqlite,
        "sqlite_rebuilt_from_jsonl_v1": rebuilt_sqlite,
        "nokv_agent_index_v1": {
            "path": options.agent_index,
            "bytes": dir_size(&options.agent_index)?,
            "ingest_ms": round_ms(ingest_ms),
            "ingest_report": ingest_report,
            "queries": agent_queries,
        },
        "comparisons": comparisons,
        "projected_comparisons": projected_comparisons,
        "rebuilt_comparisons": rebuilt_comparisons,
        "notes": [
            "JSONL is the source of truth; nokv_agent_index_v1 is a rebuildable derived index.",
            "sqlite_rebuilt_from_jsonl_v1 is emitted only when --rebuilt-sqlite PATH is provided and uses the same events.jsonl input as nokv_agent_index_v1.",
            "Fingerprints hash only type, timestamp, source offset, and aggregate labels; raw user text and tool arguments are not printed.",
            "sqlite_projected_v1 is emitted only when --projected-sqlite PATH is provided; use it before making optimization claims."
        ],
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn parse_args() -> BenchResult<Options> {
    let mut args = std::env::args().skip(1).collect::<VecDeque<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Err(usage().into());
    }
    let events_jsonl = required_path(&mut args, "--events-jsonl")?;
    let sqlite = required_path(&mut args, "--sqlite")?;
    let projected_sqlite = option_value(&mut args, "--projected-sqlite")?.map(PathBuf::from);
    let rebuilt_sqlite = option_value(&mut args, "--rebuilt-sqlite")?.map(PathBuf::from);
    let agent_index = required_path(&mut args, "--agent-index")?;
    let source_file =
        option_value(&mut args, "--source-file")?.unwrap_or_else(|| "logs/events.jsonl".to_owned());
    let agent_id = option_value(&mut args, "--agent-id")?.unwrap_or_else(|| "default".to_owned());
    let iterations = option_value(&mut args, "--iterations")?
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(20);
    let reset = take_flag(&mut args, "--reset");
    if !args.is_empty() {
        return Err(format!("unexpected argument {}", args.front().unwrap()).into());
    }
    Ok(Options {
        events_jsonl,
        sqlite,
        projected_sqlite,
        rebuilt_sqlite,
        agent_index,
        source_file,
        agent_id,
        iterations,
        reset,
    })
}

fn sqlite_queries(
    conn: &Connection,
    iterations: usize,
) -> BenchResult<BTreeMap<String, QueryStats>> {
    let notification_pivots = sqlite_notification_pivots(conn)?;
    let mut queries = BTreeMap::new();
    queries.insert(
        "coverage".to_owned(),
        timed(iterations, || sqlite_coverage(conn))?,
    );
    queries.insert(
        "stream_session_rows".to_owned(),
        timed(iterations, || sqlite_session_rows(conn))?,
    );
    queries.insert(
        "latest_notification_block_10".to_owned(),
        timed(iterations, || {
            sqlite_latest_by_type(conn, "notification_block_injected", 10)
        })?,
    );
    queries.insert(
        "latest_notification_pair_10".to_owned(),
        timed(iterations, || {
            sqlite_latest_by_type(conn, "notification_pair_injected", 10)
        })?,
    );
    queries.insert(
        "notification_events_50".to_owned(),
        timed(iterations, || sqlite_notification_events(conn, 50))?,
    );
    queries.insert(
        "notification_by_latest_id".to_owned(),
        timed(iterations, || {
            sqlite_notification_by_id(conn, notification_pivots.latest)
        })?,
    );
    queries.insert(
        "notification_before_latest".to_owned(),
        timed(iterations, || {
            sqlite_notification_before(conn, notification_pivots.latest)
        })?,
    );
    queries.insert(
        "notification_after_earliest".to_owned(),
        timed(iterations, || {
            sqlite_notification_after(conn, notification_pivots.earliest)
        })?,
    );
    queries.insert(
        "recent_psyche_molt_10".to_owned(),
        timed(iterations, || sqlite_recent_times(conn, "psyche_molt", 10))?,
    );
    queries.insert(
        "molt_session_windows".to_owned(),
        timed(iterations, || sqlite_molt_session_windows(conn))?,
    );
    queries.insert(
        "recent_refresh_complete_10".to_owned(),
        timed(iterations, || {
            sqlite_recent_times(conn, "refresh_complete", 10)
        })?,
    );
    queries.insert(
        "tui_clear_completion_from_zero".to_owned(),
        timed(iterations, || sqlite_tui_clear_completion(conn, 0))?,
    );
    queries.insert(
        "error_events".to_owned(),
        timed(iterations, || sqlite_error_events(conn))?,
    );
    queries.insert(
        "tool_name_group_json_extract".to_owned(),
        timed(iterations, || sqlite_tool_name_facets(conn, 30))?,
    );
    queries.insert(
        "tool_action_group_json_extract".to_owned(),
        timed(iterations, || sqlite_tool_action_facets(conn, 50))?,
    );
    Ok(queries)
}

fn projected_sqlite_baseline(
    source: &Path,
    projected: &Path,
    iterations: usize,
) -> BenchResult<ProjectedSqliteStats> {
    if source == projected {
        return Err("--projected-sqlite must not point at --sqlite".into());
    }
    if let Some(parent) = projected.parent() {
        fs::create_dir_all(parent)?;
    }
    if projected.exists() {
        fs::remove_file(projected)?;
    }
    let started = Instant::now();
    fs::copy(source, projected)?;
    let mut conn = Connection::open(projected)?;
    create_projected_sqlite_indexes(&mut conn)?;
    let build_ms = elapsed_ms(started);
    conn.execute_batch("pragma query_only=on")?;
    let queries = projected_sqlite_queries(&conn, iterations)?;
    Ok(ProjectedSqliteStats {
        path: projected.to_path_buf(),
        bytes: projected.metadata()?.len(),
        build_ms: round_ms(build_ms),
        sqlite_version: conn.query_row("select sqlite_version()", [], |row| row.get(0))?,
        projected_index_sizes: sqlite_index_sizes(&conn)?,
        queries,
    })
}

fn rebuilt_sqlite_baseline(
    options: &Options,
    rebuilt: &Path,
    iterations: usize,
) -> BenchResult<RebuiltSqliteStats> {
    if let Some(parent) = rebuilt.parent() {
        fs::create_dir_all(parent)?;
    }
    if rebuilt.exists() {
        fs::remove_file(rebuilt)?;
    }

    let started = Instant::now();
    let mut conn = Connection::open(rebuilt)?;
    conn.execute_batch(
        "
        pragma journal_mode=off;
        pragma synchronous=off;
        create table events (
          id integer primary key autoincrement,
          ts real not null,
          type text not null,
          agent_address text,
          agent_name_snapshot text,
          fields_json text not null,
          source_file text,
          source_offset integer,
          source_line integer,
          source_kind text,
          scope text,
          run_id text,
          inserted_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );
        ",
    )?;
    let event_rows = insert_jsonl_sqlite_events(&mut conn, options)?;
    create_projected_sqlite_indexes(&mut conn)?;
    let build_ms = elapsed_ms(started);

    conn.execute_batch("pragma query_only=on")?;
    let queries = projected_sqlite_queries(&conn, iterations)?;
    Ok(RebuiltSqliteStats {
        path: rebuilt.to_path_buf(),
        bytes: rebuilt.metadata()?.len(),
        build_ms: round_ms(build_ms),
        sqlite_version: conn.query_row("select sqlite_version()", [], |row| row.get(0))?,
        event_rows,
        projected_index_sizes: sqlite_index_sizes(&conn)?,
        queries,
    })
}

fn insert_jsonl_sqlite_events(conn: &mut Connection, options: &Options) -> BenchResult<u64> {
    let file = File::open(&options.events_jsonl)?;
    let mut reader = BufReader::new(file);
    let tx = conn.transaction()?;
    let mut statement = tx.prepare(
        "
        insert into events (ts, type, fields_json, source_file, source_offset, source_line, source_kind)
        values (?1, ?2, ?3, ?4, ?5, ?6, 'events_jsonl')
        ",
    )?;

    let mut offset = 0_u64;
    let mut line_no = 0_u64;
    let mut inserted = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        let line_offset = offset;
        offset = offset.saturating_add(read as u64);
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        if !line.ends_with(b"\n") {
            break;
        }
        line_no = line_no.saturating_add(1);
        let value: Value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(object) = value.as_object() else {
            continue;
        };
        let Some(event_type) = object.get("type").and_then(Value::as_str) else {
            continue;
        };
        let ts = object.get("ts").and_then(Value::as_f64).unwrap_or(0.0);
        let fields_json = sqlite_fields_json(object)?;
        statement.execute((
            ts,
            event_type,
            fields_json,
            options.source_file.as_str(),
            line_offset as i64,
            line_no as i64,
        ))?;
        inserted = inserted.saturating_add(1);
    }
    drop(statement);
    tx.commit()?;
    Ok(inserted)
}

fn sqlite_fields_json(object: &Map<String, Value>) -> BenchResult<String> {
    let mut fields = object.clone();
    for key in ["type", "ts", "address", "agent_name"] {
        fields.remove(key);
    }
    let value = compact_fields_json(Value::Object(fields));
    Ok(serde_json::to_string(&value)?)
}

fn compact_fields_json(value: Value) -> Value {
    let encoded_len = serde_json::to_vec(&value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if encoded_len <= MAX_INLINE_FIELDS_JSON_BYTES {
        return value;
    }
    let keys = value
        .as_object()
        .map(|object| {
            object
                .keys()
                .cloned()
                .map(Value::String)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "_nokv_compacted": true,
        "original_json_bytes": encoded_len,
        "keys": keys,
    })
}

fn create_projected_sqlite_indexes(conn: &mut Connection) -> BenchResult<()> {
    let session_types = LINGTAI_SESSION_EVENT_TYPES
        .iter()
        .map(|value| sql_string_literal(value))
        .collect::<Vec<_>>()
        .join(",");
    let tx = conn.transaction()?;
    tx.execute_batch(&format!(
        "
        create index if not exists idx_events_source_offset_only
          on events(source_offset)
          where source_offset is not null;
        create index if not exists idx_events_session_id
          on events(id)
          where type in ({session_types});
        create index if not exists idx_events_type_id_desc
          on events(type, id desc);
        create index if not exists idx_events_type_ts_desc
          on events(type, ts desc);
        create index if not exists idx_events_notification_id_desc
          on events(id desc)
          where type like '%notification%';
        create index if not exists idx_events_tui_clear_id_desc
          on events(id desc)
          where type in ('psyche_molt','clear_received')
            and source_offset is not null
            and json_valid(fields_json)
            and json_extract(fields_json, '$.source') = 'tui';
        create index if not exists idx_events_tool_name_expr
          on events(coalesce(json_extract(fields_json, '$.tool_name'), ''))
          where type = 'tool_call'
            and json_valid(fields_json)
            and json_extract(fields_json, '$.tool_name') is not null;
        create index if not exists idx_events_tool_action_expr
          on events(
            coalesce(json_extract(fields_json, '$.tool_name'), ''),
            coalesce(json_extract(fields_json, '$.tool_args.action'), '')
          )
          where type = 'tool_call'
            and json_valid(fields_json)
            and json_extract(fields_json, '$.tool_name') is not null;
        "
    ))?;
    tx.commit()?;
    Ok(())
}

fn projected_sqlite_queries(
    conn: &Connection,
    iterations: usize,
) -> BenchResult<BTreeMap<String, QueryStats>> {
    let mut queries = sqlite_queries(conn, iterations)?;
    queries.insert(
        "tool_name_group_json_extract".to_owned(),
        timed(iterations, || sqlite_tool_name_facets_indexed(conn, 30))?,
    );
    queries.insert(
        "tool_action_group_json_extract".to_owned(),
        timed(iterations, || sqlite_tool_action_facets_indexed(conn, 50))?,
    );
    Ok(queries)
}

fn sqlite_index_sizes(conn: &Connection) -> BenchResult<Vec<SqliteIndexSize>> {
    let mut out = Vec::new();
    let mut statement = match conn.prepare(
        "select name, coalesce(sum(pgsize), 0) \
         from dbstat where name like 'idx_events_%' group by name order by name",
    ) {
        Ok(statement) => statement,
        Err(_) => return Ok(out),
    };
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let bytes: i64 = row.get(1)?;
        out.push(SqliteIndexSize {
            name,
            bytes: bytes.max(0) as u64,
        });
    }
    Ok(out)
}

fn agent_queries(
    store: &HoltAgentEventStore,
    options: &Options,
    iterations: usize,
) -> BenchResult<BTreeMap<String, QueryStats>> {
    let notification_pivots = agent_notification_pivots(store, options)?;
    let mut queries = BTreeMap::new();
    queries.insert(
        "coverage".to_owned(),
        timed(iterations, || agent_coverage(store, options))?,
    );
    queries.insert(
        "stream_session_rows".to_owned(),
        timed(iterations, || agent_session_rows(store, options))?,
    );
    queries.insert(
        "stream_session_events_full".to_owned(),
        timed(iterations, || agent_session_events(store, options))?,
    );
    queries.insert(
        "latest_notification_block_10".to_owned(),
        timed(iterations, || {
            agent_latest_by_type(store, options, "notification_block_injected", 10)
        })?,
    );
    queries.insert(
        "latest_notification_pair_10".to_owned(),
        timed(iterations, || {
            agent_latest_by_type(store, options, "notification_pair_injected", 10)
        })?,
    );
    queries.insert(
        "notification_events_50".to_owned(),
        timed(iterations, || agent_notification_events(store, options, 50))?,
    );
    queries.insert(
        "notification_by_latest_id".to_owned(),
        timed(iterations, || {
            agent_notification_by_id(store, options, notification_pivots.latest)
        })?,
    );
    queries.insert(
        "notification_before_latest".to_owned(),
        timed(iterations, || {
            agent_notification_before(store, options, notification_pivots.latest)
        })?,
    );
    queries.insert(
        "notification_after_earliest".to_owned(),
        timed(iterations, || {
            agent_notification_after(store, options, notification_pivots.earliest)
        })?,
    );
    queries.insert(
        "recent_psyche_molt_10".to_owned(),
        timed(iterations, || {
            agent_recent_times(store, options, "psyche_molt", 10)
        })?,
    );
    queries.insert(
        "molt_session_windows".to_owned(),
        timed(iterations, || agent_molt_session_windows(store, options))?,
    );
    queries.insert(
        "recent_refresh_complete_10".to_owned(),
        timed(iterations, || {
            agent_recent_times(store, options, "refresh_complete", 10)
        })?,
    );
    queries.insert(
        "tui_clear_completion_from_zero".to_owned(),
        timed(iterations, || agent_tui_clear_completion(store, options, 0))?,
    );
    queries.insert(
        "error_events".to_owned(),
        timed(iterations, || agent_error_events(store, options))?,
    );
    queries.insert(
        "tool_name_group_json_extract".to_owned(),
        timed(iterations, || agent_tool_name_facets(store, options, 30))?,
    );
    queries.insert(
        "tool_action_group_json_extract".to_owned(),
        timed(iterations, || agent_tool_action_facets(store, options, 50))?,
    );
    Ok(queries)
}

fn sqlite_coverage(conn: &Connection) -> BenchResult<QueryDigest> {
    let row = conn.query_row(
        "select coalesce(min(source_offset), -1), coalesce(max(source_offset), -1), count(source_offset) from events",
        [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
    )?;
    let mut digest = SafeDigest::new();
    digest.field("min", row.0);
    digest.field("max", row.1);
    digest.field("count", row.2);
    Ok(QueryDigest {
        rows: 1,
        fingerprint: digest.finish(),
    })
}

fn sqlite_session_rows(conn: &Connection) -> BenchResult<QueryDigest> {
    let placeholders = placeholders(LINGTAI_SESSION_EVENT_TYPES.len());
    let sql = format!(
        "select ts, type, fields_json from events where type in ({placeholders}) order by id asc"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(params_from_iter(LINGTAI_SESSION_EVENT_TYPES.iter()))?;
    let mut count = 0_usize;
    let mut digest = SafeDigest::new();
    while let Some(row) = rows.next()? {
        let ts: f64 = row.get(0)?;
        let event_type: String = row.get(1)?;
        let _fields_json: String = row.get(2)?;
        digest.event(&event_type, ts, -1);
        count += 1;
    }
    Ok(QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    })
}

fn sqlite_latest_by_type(
    conn: &Connection,
    event_type: &str,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select type, ts, source_offset from events where type = ?1 order by id desc limit {limit}"
    );
    sqlite_event_digest(conn, &sql, &[event_type])
}

fn sqlite_notification_events(conn: &Connection, limit: usize) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select type, ts, source_offset from events \
         where type like '%notification%' order by id desc limit {limit}"
    );
    sqlite_event_digest(conn, &sql, &[])
}

fn sqlite_notification_pivots(conn: &Connection) -> BenchResult<NotificationPivots> {
    let latest = conn.query_row(
        "select max(id) from events where type like '%notification%'",
        [],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    let earliest = conn.query_row(
        "select min(id) from events where type like '%notification%'",
        [],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    Ok(NotificationPivots {
        latest: latest.map(|id| id as u64),
        earliest: earliest.map(|id| id as u64),
    })
}

fn sqlite_notification_by_id(conn: &Connection, event_id: Option<u64>) -> BenchResult<QueryDigest> {
    let Some(event_id) = event_id else {
        return Ok(empty_digest());
    };
    let sql = format!("select type, ts, source_offset from events where id = {event_id}");
    sqlite_event_digest(conn, &sql, &[])
}

fn sqlite_notification_before(
    conn: &Connection,
    pivot_event_id: Option<u64>,
) -> BenchResult<QueryDigest> {
    let Some(pivot_event_id) = pivot_event_id else {
        return Ok(empty_digest());
    };
    let sql = format!(
        "select type, ts, source_offset from events \
         where type like '%notification%' and id < {pivot_event_id} \
         order by id desc limit 1"
    );
    sqlite_event_digest(conn, &sql, &[])
}

fn sqlite_notification_after(
    conn: &Connection,
    pivot_event_id: Option<u64>,
) -> BenchResult<QueryDigest> {
    let Some(pivot_event_id) = pivot_event_id else {
        return Ok(empty_digest());
    };
    let sql = format!(
        "select type, ts, source_offset from events \
         where type like '%notification%' and id > {pivot_event_id} \
         order by id asc limit 1"
    );
    sqlite_event_digest(conn, &sql, &[])
}

fn sqlite_recent_times(
    conn: &Connection,
    event_type: &str,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let sql =
        format!("select type, ts, -1 from events where type = ?1 order by ts desc limit {limit}");
    sqlite_event_digest(conn, &sql, &[event_type])
}

fn sqlite_molt_session_windows(conn: &Connection) -> BenchResult<QueryDigest> {
    let mut statement =
        conn.prepare("select ts from events where type = 'psyche_molt' order by ts desc limit 2")?;
    let mut rows = statement.query([])?;
    let mut times = Vec::new();
    while let Some(row) = rows.next()? {
        times.push(row.get::<_, f64>(0)?);
    }
    let current_since = times.first().copied();
    let last_since = times.get(1).copied();
    let last_before = last_since.and(current_since);
    let mut digest = SafeDigest::new();
    digest.field("ok", 1);
    digest.field(
        "current_since",
        current_since.map(timestamp_micros_i64).unwrap_or(-1),
    );
    digest.field(
        "last_since",
        last_since.map(timestamp_micros_i64).unwrap_or(-1),
    );
    digest.field(
        "last_before",
        last_before.map(timestamp_micros_i64).unwrap_or(-1),
    );
    Ok(QueryDigest {
        rows: 1,
        fingerprint: digest.finish(),
    })
}

fn sqlite_tui_clear_completion(conn: &Connection, source_offset: u64) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select type, ts, source_offset from events \
         where type in ('psyche_molt','clear_received') \
         and source_offset >= {source_offset} \
         and json_valid(fields_json) \
         and json_extract(fields_json, '$.source') = 'tui' \
         order by id desc limit 1"
    );
    sqlite_event_digest(conn, &sql, &[])
}

fn sqlite_error_events(conn: &Connection) -> BenchResult<QueryDigest> {
    let placeholders = placeholders(ERROR_EVENT_TYPES.len());
    let sql = format!(
        "select type, ts, source_offset from events where type in ({placeholders}) order by id desc"
    );
    sqlite_event_digest(conn, &sql, ERROR_EVENT_TYPES)
}

fn sqlite_event_digest(conn: &Connection, sql: &str, params: &[&str]) -> BenchResult<QueryDigest> {
    let mut statement = conn.prepare(sql)?;
    let mut rows = statement.query(params_from_iter(params.iter()))?;
    let mut count = 0_usize;
    let mut digest = SafeDigest::new();
    while let Some(row) = rows.next()? {
        let event_type: String = row.get(0)?;
        let ts: f64 = row.get(1)?;
        let source_offset: Option<i64> = row.get(2)?;
        digest.event(&event_type, ts, source_offset.unwrap_or(-1));
        count += 1;
    }
    Ok(QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    })
}

fn sqlite_tool_name_facets(conn: &Connection, limit: usize) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select coalesce(json_extract(fields_json, '$.tool_name'), ''), count(*) \
         from events where type = 'tool_call' and json_valid(fields_json) \
         and json_extract(fields_json, '$.tool_name') is not null \
         group by 1 order by count(*) desc, 1 asc limit {limit}"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query([])?;
    let mut count = 0_usize;
    let mut digest = SafeDigest::new();
    while let Some(row) = rows.next()? {
        let tool_name: String = row.get(0)?;
        let n: i64 = row.get(1)?;
        digest.facet(&tool_name, None, n as u64);
        count += 1;
    }
    Ok(QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    })
}

fn sqlite_tool_name_facets_indexed(conn: &Connection, limit: usize) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select coalesce(json_extract(fields_json, '$.tool_name'), ''), count(*) \
         from events indexed by idx_events_tool_name_expr \
         where type = 'tool_call' and json_valid(fields_json) \
         and json_extract(fields_json, '$.tool_name') is not null \
         group by 1 order by count(*) desc, 1 asc limit {limit}"
    );
    sqlite_facet_digest(conn, &sql, false)
}

fn sqlite_tool_action_facets(conn: &Connection, limit: usize) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select coalesce(json_extract(fields_json, '$.tool_name'), ''), \
         coalesce(json_extract(fields_json, '$.tool_args.action'), ''), count(*) \
         from events where type = 'tool_call' and json_valid(fields_json) \
         and json_extract(fields_json, '$.tool_name') is not null \
         group by 1, 2 order by count(*) desc, 1 asc, 2 asc limit {limit}"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query([])?;
    let mut count = 0_usize;
    let mut digest = SafeDigest::new();
    while let Some(row) = rows.next()? {
        let tool_name: String = row.get(0)?;
        let action: String = row.get(1)?;
        let n: i64 = row.get(2)?;
        digest.facet(&tool_name, Some(&action), n as u64);
        count += 1;
    }
    Ok(QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    })
}

fn sqlite_tool_action_facets_indexed(conn: &Connection, limit: usize) -> BenchResult<QueryDigest> {
    let sql = format!(
        "select coalesce(json_extract(fields_json, '$.tool_name'), ''), \
         coalesce(json_extract(fields_json, '$.tool_args.action'), ''), count(*) \
         from events indexed by idx_events_tool_action_expr \
         where type = 'tool_call' and json_valid(fields_json) \
         and json_extract(fields_json, '$.tool_name') is not null \
         group by 1, 2 order by count(*) desc, 1 asc, 2 asc limit {limit}"
    );
    sqlite_facet_digest(conn, &sql, true)
}

fn sqlite_facet_digest(
    conn: &Connection,
    sql: &str,
    include_action: bool,
) -> BenchResult<QueryDigest> {
    let mut statement = conn.prepare(sql)?;
    let mut rows = statement.query([])?;
    let mut count = 0_usize;
    let mut digest = SafeDigest::new();
    while let Some(row) = rows.next()? {
        let tool_name: String = row.get(0)?;
        if include_action {
            let action: String = row.get(1)?;
            let n: i64 = row.get(2)?;
            digest.facet(&tool_name, Some(&action), n as u64);
        } else {
            let n: i64 = row.get(1)?;
            digest.facet(&tool_name, None, n as u64);
        }
        count += 1;
    }
    Ok(QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    })
}

fn agent_coverage(store: &HoltAgentEventStore, options: &Options) -> BenchResult<QueryDigest> {
    let coverage = store
        .coverage(&options.agent_id, &options.source_file)?
        .ok_or("agent coverage is missing")?;
    let mut digest = SafeDigest::new();
    digest.field(
        "min",
        coverage.min_offset.map(|value| value as i64).unwrap_or(-1),
    );
    digest.field(
        "max",
        coverage.max_offset.map(|value| value as i64).unwrap_or(-1),
    );
    digest.field("count", coverage.row_count as i64);
    Ok(QueryDigest {
        rows: 1,
        fingerprint: digest.finish(),
    })
}

fn agent_session_rows(store: &HoltAgentEventStore, options: &Options) -> BenchResult<QueryDigest> {
    let rows = store.stream_session_rows(SessionRowsRequest {
        agent_id: options.agent_id.clone(),
        limit: None,
    })?;
    Ok(agent_session_row_digest(rows))
}

fn agent_session_events(
    store: &HoltAgentEventStore,
    options: &Options,
) -> BenchResult<QueryDigest> {
    let records = store.stream_session_events(SessionEventsRequest {
        agent_id: options.agent_id.clone(),
        event_types: LINGTAI_SESSION_EVENT_TYPES
            .iter()
            .map(|value| value.to_string())
            .collect(),
        limit: None,
    })?;
    Ok(agent_event_digest(records))
}

fn agent_latest_by_type(
    store: &HoltAgentEventStore,
    options: &Options,
    event_type: &str,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let records = store.latest_events(LatestEventsRequest {
        agent_id: options.agent_id.clone(),
        event_type: event_type.to_owned(),
        limit,
    })?;
    Ok(agent_event_digest(records))
}

fn agent_notification_events(
    store: &HoltAgentEventStore,
    options: &Options,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let records = store.notification_events(NotificationEventsRequest {
        agent_id: options.agent_id.clone(),
        limit,
    })?;
    Ok(agent_event_digest(records))
}

fn agent_notification_pivots(
    store: &HoltAgentEventStore,
    options: &Options,
) -> BenchResult<NotificationPivots> {
    let mut events = store.notification_events(NotificationEventsRequest {
        agent_id: options.agent_id.clone(),
        limit: usize::MAX,
    })?;
    Ok(NotificationPivots {
        latest: events.first().map(|record| record.id),
        earliest: events.pop().map(|record| record.id),
    })
}

fn agent_notification_by_id(
    store: &HoltAgentEventStore,
    options: &Options,
    event_id: Option<u64>,
) -> BenchResult<QueryDigest> {
    let Some(event_id) = event_id else {
        return Ok(empty_digest());
    };
    let event = store.notification_event_by_id(NotificationEventByIdRequest {
        agent_id: options.agent_id.clone(),
        event_id,
    })?;
    Ok(agent_event_digest(event.into_iter().collect()))
}

fn agent_notification_before(
    store: &HoltAgentEventStore,
    options: &Options,
    pivot_event_id: Option<u64>,
) -> BenchResult<QueryDigest> {
    let Some(pivot_event_id) = pivot_event_id else {
        return Ok(empty_digest());
    };
    let event = store.notification_neighbor(NotificationNeighborRequest {
        agent_id: options.agent_id.clone(),
        pivot_event_id,
        direction: NotificationNeighborDirection::Before,
    })?;
    Ok(agent_event_digest(event.into_iter().collect()))
}

fn agent_notification_after(
    store: &HoltAgentEventStore,
    options: &Options,
    pivot_event_id: Option<u64>,
) -> BenchResult<QueryDigest> {
    let Some(pivot_event_id) = pivot_event_id else {
        return Ok(empty_digest());
    };
    let event = store.notification_neighbor(NotificationNeighborRequest {
        agent_id: options.agent_id.clone(),
        pivot_event_id,
        direction: NotificationNeighborDirection::After,
    })?;
    Ok(agent_event_digest(event.into_iter().collect()))
}

fn agent_recent_times(
    store: &HoltAgentEventStore,
    options: &Options,
    event_type: &str,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let times = store.recent_times(RecentTimesRequest {
        agent_id: options.agent_id.clone(),
        event_type: event_type.to_owned(),
        limit,
    })?;
    let mut digest = SafeDigest::new();
    for item in &times {
        digest.event(event_type, item.ts, -1);
    }
    Ok(QueryDigest {
        rows: times.len(),
        fingerprint: digest.finish(),
    })
}

fn agent_molt_session_windows(
    store: &HoltAgentEventStore,
    options: &Options,
) -> BenchResult<QueryDigest> {
    let windows = store.molt_session_windows(&options.agent_id)?;
    let mut digest = SafeDigest::new();
    digest.field("ok", i64::from(windows.ok));
    digest.field(
        "current_since",
        windows
            .current_since
            .map(timestamp_micros_i64)
            .unwrap_or(-1),
    );
    digest.field(
        "last_since",
        windows.last_since.map(timestamp_micros_i64).unwrap_or(-1),
    );
    digest.field(
        "last_before",
        windows.last_before.map(timestamp_micros_i64).unwrap_or(-1),
    );
    Ok(QueryDigest {
        rows: usize::from(windows.ok),
        fingerprint: digest.finish(),
    })
}

fn agent_tui_clear_completion(
    store: &HoltAgentEventStore,
    options: &Options,
    source_offset: u64,
) -> BenchResult<QueryDigest> {
    let completion = store.tui_clear_completion(TuiClearCompletionRequest {
        agent_id: options.agent_id.clone(),
        source_offset,
    })?;
    Ok(agent_event_digest(completion.event.into_iter().collect()))
}

fn agent_error_events(store: &HoltAgentEventStore, options: &Options) -> BenchResult<QueryDigest> {
    let records = store.error_events(ErrorEventsRequest {
        agent_id: options.agent_id.clone(),
        event_types: ERROR_EVENT_TYPES
            .iter()
            .map(|value| value.to_string())
            .collect(),
        limit: usize::MAX,
    })?;
    Ok(agent_event_digest(records))
}

fn agent_tool_name_facets(
    store: &HoltAgentEventStore,
    options: &Options,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let action_facets = store.tool_facets(ToolFacetRequest {
        agent_id: options.agent_id.clone(),
        limit: usize::MAX,
    })?;
    let mut counts = BTreeMap::<String, u64>::new();
    for facet in action_facets {
        *counts.entry(facet.tool_name).or_default() += facet.count;
    }
    let mut facets = counts
        .into_iter()
        .map(|(tool_name, count)| ToolFacet {
            tool_name,
            action: None,
            count,
        })
        .collect::<Vec<_>>();
    sort_facets(&mut facets);
    facets.truncate(limit);
    Ok(agent_facet_digest(&facets))
}

fn agent_tool_action_facets(
    store: &HoltAgentEventStore,
    options: &Options,
    limit: usize,
) -> BenchResult<QueryDigest> {
    let facets = store.tool_facets(ToolFacetRequest {
        agent_id: options.agent_id.clone(),
        limit,
    })?;
    Ok(agent_facet_digest(&facets))
}

fn agent_event_digest(records: Vec<EventRecord>) -> QueryDigest {
    let mut digest = SafeDigest::new();
    let rows = records.len();
    for record in records {
        digest.event(&record.event_type, record.ts, record.source_offset as i64);
    }
    QueryDigest {
        rows,
        fingerprint: digest.finish(),
    }
}

fn agent_session_row_digest(rows: Vec<SessionEventRow>) -> QueryDigest {
    let mut digest = SafeDigest::new();
    let count = rows.len();
    for row in rows {
        digest.event(&row.event_type, row.ts, -1);
    }
    QueryDigest {
        rows: count,
        fingerprint: digest.finish(),
    }
}

fn agent_facet_digest(facets: &[ToolFacet]) -> QueryDigest {
    let mut digest = SafeDigest::new();
    for facet in facets {
        digest.facet(&facet.tool_name, facet.action.as_deref(), facet.count);
    }
    QueryDigest {
        rows: facets.len(),
        fingerprint: digest.finish(),
    }
}

fn empty_digest() -> QueryDigest {
    QueryDigest {
        rows: 0,
        fingerprint: SafeDigest::new().finish(),
    }
}

fn timed<F>(iterations: usize, mut query: F) -> BenchResult<QueryStats>
where
    F: FnMut() -> BenchResult<QueryDigest>,
{
    let warmup = query()?;
    let mut timings = Vec::with_capacity(iterations.max(1));
    let mut last = warmup.clone();
    for _ in 0..iterations.max(1) {
        let started = Instant::now();
        last = query()?;
        timings.push(elapsed_ms(started));
    }
    timings.sort_by(|left, right| left.total_cmp(right));
    let total = timings.iter().sum::<f64>();
    let p50 = timings[timings.len() / 2];
    let p95 = timings[((timings.len() as f64 * 0.95).ceil() as usize).saturating_sub(1)]
        .min(*timings.last().unwrap());
    Ok(QueryStats {
        rows: last.rows,
        p50_ms: round_ms(p50),
        p95_ms: round_ms(p95),
        avg_ms: round_ms(total / timings.len() as f64),
        fingerprint: last.fingerprint,
    })
}

fn compare_queries(
    sqlite: &BTreeMap<String, QueryStats>,
    agent: &BTreeMap<String, QueryStats>,
) -> BTreeMap<String, QueryComparison> {
    let mut out = BTreeMap::new();
    for (name, sqlite_stats) in sqlite {
        if let Some(agent_stats) = agent.get(name) {
            out.insert(
                name.clone(),
                QueryComparison {
                    rows_match: sqlite_stats.rows == agent_stats.rows,
                    fingerprint_match: sqlite_stats.fingerprint == agent_stats.fingerprint,
                },
            );
        }
    }
    out
}

struct SafeDigest {
    hasher: Sha256,
}

impl SafeDigest {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    fn field(&mut self, label: &str, value: i64) {
        self.hasher.update(label.as_bytes());
        self.hasher.update(b"=");
        self.hasher.update(value.to_string().as_bytes());
        self.hasher.update(b"\n");
    }

    fn event(&mut self, event_type: &str, ts: f64, source_offset: i64) {
        self.hasher.update(event_type.as_bytes());
        self.hasher.update(b"|");
        self.hasher
            .update(format!("{:.6}", if ts.is_finite() { ts } else { 0.0 }).as_bytes());
        self.hasher.update(b"|");
        self.hasher.update(source_offset.to_string().as_bytes());
        self.hasher.update(b"\n");
    }

    fn facet(&mut self, tool_name: &str, action: Option<&str>, count: u64) {
        self.hasher.update(tool_name.as_bytes());
        self.hasher.update(b"|");
        self.hasher.update(action.unwrap_or("").as_bytes());
        self.hasher.update(b"|");
        self.hasher.update(count.to_string().as_bytes());
        self.hasher.update(b"\n");
    }

    fn finish(self) -> String {
        let bytes = self.hasher.finalize();
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(nibble(byte >> 4));
            out.push(nibble(byte & 0x0f));
        }
        out
    }
}

fn sort_facets(facets: &mut [ToolFacet]) {
    facets.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.tool_name.cmp(&right.tool_name))
            .then_with(|| left.action.cmp(&right.action))
    });
}

fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(",")
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

fn round_ms(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn timestamp_micros_i64(value: f64) -> i64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let micros = value * 1_000_000.0;
    if micros >= i64::MAX as f64 {
        i64::MAX
    } else {
        micros as i64
    }
}

fn dir_size(path: &Path) -> BenchResult<u64> {
    let mut bytes = 0_u64;
    if !path.exists() {
        return Ok(0);
    }
    for entry in WalkDir::new(path) {
        let entry = entry?;
        if entry.file_type().is_file() {
            bytes = bytes.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(bytes)
}

fn required_path(args: &mut VecDeque<String>, flag: &str) -> BenchResult<PathBuf> {
    required_value(args, flag).map(PathBuf::from)
}

fn required_value(args: &mut VecDeque<String>, flag: &str) -> BenchResult<String> {
    option_value(args, flag)?.ok_or_else(|| format!("missing required option {flag}").into())
}

fn option_value(args: &mut VecDeque<String>, flag: &str) -> BenchResult<Option<String>> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };
    args.remove(index);
    args.remove(index)
        .map(Some)
        .ok_or_else(|| format!("option {flag} requires a value").into())
}

fn take_flag(args: &mut VecDeque<String>, flag: &str) -> bool {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return false;
    };
    args.remove(index);
    true
}

fn nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!("nibble is four bits"),
    }
}

fn usage() -> &'static str {
    "usage: lingtai-index-bench --events-jsonl PATH --sqlite PATH --agent-index PATH [--projected-sqlite PATH] [--rebuilt-sqlite PATH] [--source-file logs/events.jsonl] [--agent-id default] [--iterations 20] [--reset]"
}
