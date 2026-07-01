use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::read::GzDecoder;
use nokv_agent::{agent_tool_definitions, execute_agent_tool};
use nokv_client::{
    ArtifactBackend, ArtifactMetadata, ArtifactRepository, ArtifactRepositoryOptions, ClientError,
};
use nokv_meta::{
    DentryWithAttr, HoltMetadataStore, MetadError, MetadataStore, NamespaceFindField,
    NamespaceIndexField, NamespaceIndexRegistration, NamespaceIndexRow, NamespaceIndexValue,
    NamespacePredicateOp, NamespacePredicateValue, NoKvFs, PreparedArtifact, PublishArtifactRange,
    PublishArtifactSession, RenameReplaceResult,
};
use nokv_object::{ObjectStore, S3ObjectStore, S3ObjectStoreOptions};
use nokv_types::{BodyDescriptor, DentryName, FileType, InodeAttr, MountId, PathMetadata};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tar::Archive;
use walkdir::WalkDir;

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;
const DEFAULT_BUCKET: &str = "nokv-yanex-demo";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
const DEFAULT_SECRET_KEY: &str = "rustfsadmin";
const RUNS_PREFIX: &str = "/runs";
const NOKV_PREFIX: &str = "yanex";
const PARAM_INDEX_FIELDS: &[(&str, &str)] = &[
    (
        "origami.training.learning_rate",
        "param.origami.training.learning_rate",
    ),
    (
        "origami.training.batch_size",
        "param.origami.training.batch_size",
    ),
];
const LATEST_METRIC_INDEX_FIELDS: &[&str] = &[
    "utility_tstr_roc_auc",
    "utility_trtr_roc_auc",
    "fidelity",
    "detection_roc_auc",
    "privacy_dcr_score",
];
const TOOL_CALL_TIMEOUT_MS: u64 = 30_000;
const SQLITE_PROGRESS_OPS: i32 = 1_000;
const RESPONSE_MAX_ATTEMPTS: usize = 10;
#[cfg(test)]
const BASE_PROFILE_YAML: &str = include_str!("../../agent-interface/base_profile.yaml");

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!(
            "\nUsage:\n  yanex-agent-bench prepare --archive PATH --data-root PATH [--reset] [s3 options]\n  yanex-agent-bench nokv-register-indexes --data-root PATH [s3 options]\n  yanex-agent-bench verify --data-root PATH [--run-id ID] [s3 options]\n  yanex-agent-bench tools --arm ARM\n  yanex-agent-bench list-tasks\n  yanex-agent-bench show-task --task-id ID\n  yanex-agent-bench gold --data-root PATH --task-id ID\n  yanex-agent-bench judge --data-root PATH --arm ARM --task-id ID --answer-json PATH\n  yanex-agent-bench run-task --data-root PATH --arm ARM --task-id ID --output-jsonl PATH [--base-profile PATH] [--api-surface SURFACE] [--model MODEL] [--max-completion-tokens N]\n  yanex-agent-bench run-batch --data-root PATH --output-jsonl PATH [--base-profile PATH] [--api-surface SURFACE] [--repeats N|--repeat N] [--arm ARM] [--task-id ID]\n  yanex-agent-bench sqlite-show-schema --db PATH\n  yanex-agent-bench sqlite-query --db PATH --sql SQL\n  yanex-agent-bench sqlite-read-blob --db PATH --blob-ref REF --offset N --limit N\n  yanex-agent-bench nokv-list|nokv-stat|nokv-read --data-root PATH --path PATH [...]\n"
        );
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), HarnessError> {
    let Some(command) = args.first().map(String::as_str) else {
        return Err(HarnessError::MissingCommand);
    };
    let options = Options::parse(&args[1..])?;
    match command {
        "prepare" => prepare(options),
        "nokv-register-indexes" => nokv_register_indexes(options),
        "verify" => verify(options).map(|report| {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).expect("verification report serializes")
            );
        }),
        "sqlite-show-schema" => show_schema(options),
        "sqlite-query" => query_sql(options),
        "sqlite-read-blob" => read_blob(options),
        "nokv-list" => nokv_list(options),
        "nokv-stat" => nokv_stat(options),
        "nokv-read" => nokv_read(options),
        "tools" => print_tool_registry(options),
        "list-tasks" => list_tasks(),
        "show-task" => show_task(options),
        "gold" => print_gold(options),
        "judge" => judge(options),
        "run-task" => run_benchmark_task(options),
        "run-batch" => run_benchmark_batch(options),
        "agent-tool" => agent_tool_debug(options),
        other => Err(HarnessError::UnknownCommand(other.to_owned())),
    }
}

#[derive(Clone, Debug, Default)]
struct Options {
    archive: Option<PathBuf>,
    data_root: Option<PathBuf>,
    db: Option<PathBuf>,
    sql: Option<String>,
    blob_ref: Option<String>,
    offset: Option<u64>,
    limit: Option<usize>,
    path: Option<String>,
    arm: Option<String>,
    task_id: Option<String>,
    output_jsonl: Option<PathBuf>,
    answer_json: Option<PathBuf>,
    base_profile: Option<PathBuf>,
    api_surface: Option<ApiSurface>,
    model: Option<String>,
    expected_generation: Option<u64>,
    max_turns: Option<usize>,
    max_tool_calls: Option<usize>,
    max_completion_tokens: Option<usize>,
    repeats: Option<usize>,
    run_id: Option<String>,
    tool_name: Option<String>,
    args_json: Option<String>,
    reset: bool,
    s3: S3Options,
}

#[derive(Clone, Debug)]
struct S3Options {
    bucket: String,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Clone, Debug)]
struct CorpusRun {
    id: String,
    metadata_bytes: Vec<u8>,
    metadata: Value,
    params_bytes: Option<Vec<u8>>,
    params: Option<serde_yaml::Value>,
    metrics_bytes: Option<Vec<u8>>,
    metrics: Option<Value>,
    dependencies_bytes: Option<Vec<u8>>,
    dependencies: Option<Value>,
    artifacts: Vec<CorpusFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchmarkTaskSet {
    task_set_id: String,
    version: u32,
    scope: String,
    tasks: Vec<BenchmarkTask>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BenchmarkTask {
    task_id: String,
    category: String,
    prompt: String,
    #[serde(default)]
    gold_sql: Option<String>,
    expected: ExpectedSpec,
}

#[derive(Clone, Debug, Deserialize)]
struct BaseProfile {
    api_surface: ApiSurface,
    model: BaseProfileModel,
    run_policy: BaseProfileRunPolicy,
    base_system_message: String,
    base_developer_message: String,
}

#[derive(Clone, Debug)]
struct LoadedBaseProfile {
    profile: BaseProfile,
    #[cfg(test)]
    yaml: String,
}

impl std::ops::Deref for LoadedBaseProfile {
    type Target = BaseProfile;

    fn deref(&self) -> &Self::Target {
        &self.profile
    }
}

#[derive(Clone, Debug, Deserialize)]
struct BaseProfileModel {
    temperature: Option<f64>,
    max_completion_tokens: usize,
    stream: bool,
    structured_output: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct BaseProfileRunPolicy {
    stateless: bool,
    repeats_per_arm_task: usize,
    max_turns: usize,
    max_tool_calls: usize,
    tool_call_timeout_ms: u64,
    retry_policy: String,
    clear_messages_after_run: bool,
    corpus_snapshot: String,
}

#[derive(Clone, Debug, PartialEq)]
struct BenchmarkRuntimeConfig {
    model: String,
    api_surface: ApiSurface,
    max_turns: usize,
    max_tool_calls: usize,
    max_completion_tokens: usize,
    tool_call_timeout: Duration,
    temperature: Option<f64>,
    response_format: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExpectedSpec {
    kind: String,
    answer_key: Option<String>,
    #[serde(default)]
    run_id_column: Option<String>,
    #[serde(default)]
    columns: Vec<String>,
}

#[derive(Clone, Debug)]
struct CorpusFile {
    relative_path: String,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
struct DerivedIndexes {
    files: BTreeMap<String, Vec<u8>>,
    status_counts: BTreeMap<String, usize>,
    tag_counts: BTreeMap<String, usize>,
    script_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LrBatchKey {
    learning_rate: String,
    batch_size: String,
}

#[derive(Clone, Debug)]
struct LrBatchGroupSummary {
    key: LrBatchKey,
    run_count: usize,
    representative_run_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct RunSummary {
    experiment_id: String,
    name: Option<String>,
    status: Option<String>,
    project: Option<String>,
    script: Option<String>,
    created_at: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct PrepareManifest {
    archive_sha256: String,
    run_count: usize,
    metadata_files: usize,
    metrics_files: usize,
    params_files: usize,
    dependencies_files: usize,
    artifact_files: usize,
    missing_artifact_refs: usize,
    sqlite_db: String,
    nokv_meta: String,
    status_counts: BTreeMap<String, usize>,
    tag_counts: BTreeMap<String, usize>,
    script_counts: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Serialize)]
struct VerificationReport {
    run_id_sample: String,
    sqlite: SqliteMaterializationReport,
    nokv_native: NamespaceMaterializationReport,
    all_available_reads_match: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct SqliteMaterializationReport {
    run_count: usize,
    raw_metadata_checked: usize,
    raw_params_checked: usize,
    raw_metrics_checked: usize,
    raw_dependencies_checked: usize,
    raw_existing_artifacts_checked: usize,
    raw_missing_artifacts_checked: usize,
    sqlite_files_checked: usize,
    index_files_checked: usize,
    agent_index_rows_checked: usize,
    agent_index_values_checked: usize,
    agent_index_catalog_checked: usize,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct NamespaceMaterializationReport {
    files_checked: usize,
    agent_catalog_fields_checked: usize,
    run_files_checked: usize,
    index_files_checked: usize,
    missing_artifacts_checked: usize,
    metadata_generations_observed: usize,
    body_descriptors_checked: usize,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteQueryToolResult {
    tool: &'static str,
    row_limit: usize,
    truncated: bool,
    rows: Vec<SqliteQueryToolRow>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteQueryToolRow {
    row: Value,
    evidence: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SqliteReadBlobToolResult {
    tool: &'static str,
    blob_ref: String,
    evidence: String,
    total_size_bytes: usize,
    digest: String,
    offset: u64,
    requested_limit: usize,
    bytes_read: usize,
    content_utf8: Option<String>,
    content_hex: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ToolDefinition {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApiSurface {
    ChatCompletions,
    ResponsesLegacy,
    AgentsResponsesSchemaOnce,
}

#[derive(Clone, Debug, Serialize)]
struct FileEntry {
    name: String,
    path: String,
    file_type: String,
    size_bytes: Option<u64>,
    digest: Option<String>,
    evidence: String,
    inode: Option<u64>,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    modified_ms: Option<i64>,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct BodyDescriptorSummary {
    producer: String,
    digest_uri: String,
    size: u64,
    content_type: String,
    manifest_id: String,
    generation: u64,
    chunk_size: u64,
    block_size: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ListToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    entries: Vec<FileEntry>,
    row_limit: usize,
    truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
struct StatToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    file_type: String,
    size_bytes: Option<u64>,
    digest: Option<String>,
    inode: Option<u64>,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    modified_ms: Option<i64>,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct ReadToolResult {
    tool: &'static str,
    path: String,
    evidence: String,
    total_size_bytes: u64,
    digest: Option<String>,
    offset: u64,
    requested_limit: usize,
    bytes_read: usize,
    content_utf8: Option<String>,
    content_hex: String,
    generation: Option<u64>,
    body: Option<BodyDescriptorSummary>,
    generation_mismatch: Option<GenerationMismatch>,
}

#[derive(Clone, Debug, Serialize)]
struct GenerationMismatch {
    expected: u64,
    current: u64,
}

#[derive(Clone, Debug, Serialize)]
struct JudgeResult {
    task_success: bool,
    evidence_precision: Option<f64>,
    evidence_checked: usize,
    evidence_supported: usize,
    expected_run_ids: Vec<String>,
    actual_run_ids: Vec<String>,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkRunTelemetry {
    record_type: String,
    run_id: String,
    arm_id: String,
    task_id: String,
    repeat_index: usize,
    model: String,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    api_calls: Vec<ApiCallTelemetry>,
    tool_calls: Vec<ToolCallTelemetry>,
    derived_metrics: DerivedMetrics,
    correctness: bool,
    judge: Option<JudgeResult>,
    final_answer: Option<Value>,
    run_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ToolCallStartTelemetry {
    record_type: String,
    run_id: String,
    arm_id: String,
    task_id: String,
    repeat_index: usize,
    call_id: String,
    tool_name: String,
    arguments: Value,
    started_at_unix_ms: u128,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ApiCallTelemetry {
    request_id: Option<String>,
    response_id: Option<String>,
    model: String,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    previous_response_id: Option<String>,
    sent_tool_schema: bool,
    sent_initial_instructions: bool,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    total_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    cached_prompt_tokens: Option<u64>,
    accepted_prediction_tokens: Option<u64>,
    rejected_prediction_tokens: Option<u64>,
}

#[derive(Clone, Debug, Serialize)]
struct AgentRunnerConfig {
    run_id: String,
    model: String,
    endpoint: String,
    arm: String,
    task_id: String,
    max_turns: usize,
    max_completion_tokens: usize,
    temperature: Option<f64>,
    response_format: Option<Value>,
    messages: Vec<ChatMessage>,
    tool_bridge_url: String,
    tools: Vec<ToolDefinition>,
}

#[derive(Clone, Debug, Deserialize)]
struct AgentRunnerOutput {
    final_answer: Option<Value>,
    final_output: Option<String>,
    run_error: Option<String>,
    api_calls: Vec<ApiCallTelemetry>,
}

#[derive(Clone, Debug, Serialize)]
struct ToolCallTelemetry {
    call_id: String,
    tool_name: String,
    arguments: Value,
    started_at_unix_ms: u128,
    completed_at_unix_ms: u128,
    status: String,
    bytes_read: usize,
    result_token_estimate: usize,
    error: Option<String>,
    timed_out: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
struct DerivedMetrics {
    interface_card_tokens: usize,
    task_prompt_tokens: usize,
    execution_cost_usd: Option<f64>,
    all_in_cost_usd: Option<f64>,
    wall_time_ms: u128,
    task_success: Option<bool>,
    evidence_precision: Option<f64>,
    tool_call_count: usize,
    tool_result_tokens: usize,
    tool_bytes_read: usize,
    invalid_sql_count: usize,
    wrong_tool_count: usize,
    tool_timeout_count: usize,
    missing_evidence_count: usize,
    overread_bytes: usize,
    tool_schema_repeated_count: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ResponseRequest {
    model: String,
    input: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ResponseTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_response_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    max_output_tokens: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<ResponseTextConfig>,
}

#[derive(Clone, Debug, Serialize)]
struct ResponseTextConfig {
    format: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Clone, Debug, Serialize)]
struct ResponseTool {
    #[serde(rename = "type")]
    tool_type: String,
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiToolCallFunction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpenAiToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ResponseApiResponse {
    id: Option<String>,
    model: Option<String>,
    #[serde(default)]
    output: Vec<Value>,
    output_text: Option<String>,
    usage: Option<ResponseUsage>,
}

#[derive(Clone, Debug, Deserialize)]
struct ResponseUsage {
    #[serde(alias = "prompt_tokens")]
    input_tokens: Option<u64>,
    #[serde(alias = "completion_tokens")]
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    #[serde(alias = "prompt_tokens_details")]
    input_tokens_details: Option<PromptTokenDetails>,
    #[serde(alias = "completion_tokens_details")]
    output_tokens_details: Option<CompletionTokenDetails>,
}

#[derive(Clone, Debug, Deserialize)]
struct PromptTokenDetails {
    cached_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct CompletionTokenDetails {
    reasoning_tokens: Option<u64>,
    accepted_prediction_tokens: Option<u64>,
    rejected_prediction_tokens: Option<u64>,
}

struct ToolExecutionOutcome {
    content: Value,
    bytes_read: usize,
    result_token_estimate: usize,
    status: String,
    error: Option<String>,
    invalid_sql: bool,
    wrong_tool: bool,
    timed_out: bool,
    fatal_run_error: bool,
}

enum ArmRuntime {
    SqliteRaw {
        conn: Connection,
    },
    NokvNative {
        service: Box<NoKvFs<HoltMetadataStore, S3ObjectStore>>,
    },
}

struct ToolWorker {
    request_tx: mpsc::Sender<ToolWorkerRequest>,
    timeout: Duration,
}

struct ToolWorkerRequest {
    call: OpenAiToolCall,
    response_tx: mpsc::Sender<ToolExecutionOutcome>,
}

struct ToolBridgeServer {
    url: String,
    shutdown_tx: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<Result<ToolBridgeSnapshot, HarnessError>>>,
}

struct ToolBridgeStartConfig<'a> {
    run_id: &'a str,
    arm: &'a str,
    task_id: &'a str,
    repeat_index: usize,
    data_root: &'a Path,
    s3: &'a S3Options,
    registry: &'a [ToolDefinition],
    timeout: Duration,
    output_jsonl: PathBuf,
}

struct ToolBridgeConnectionContext<'a> {
    run_id: &'a str,
    arm: &'a str,
    task_id: &'a str,
    repeat_index: usize,
    output_jsonl: &'a Path,
    tool_worker: &'a ToolWorker,
}

#[derive(Clone, Debug, Default)]
struct ToolBridgeSnapshot {
    tool_calls: Vec<ToolCallTelemetry>,
    invalid_sql_count: usize,
    wrong_tool_count: usize,
    tool_timeout_count: usize,
    tool_bytes_read: usize,
    tool_result_tokens: usize,
    fatal_error: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ToolBridgeInvokeRequest {
    run_id: String,
    tool_name: String,
    arguments: Value,
}

#[derive(Clone, Debug, Serialize)]
struct ToolBridgeInvokeResponse {
    status: String,
    content: Value,
    bytes_read: usize,
    result_token_estimate: usize,
    error: Option<String>,
    timed_out: bool,
    fatal_run_error: bool,
}

struct HttpRequest {
    path: String,
    body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BatchPlanItem {
    arm_id: String,
    task_id: String,
    repeat_index: usize,
}

#[derive(Debug)]
enum HarnessError {
    MissingCommand,
    UnknownCommand(String),
    MissingOption(&'static str),
    UnknownOption(String),
    InvalidNumber { option: &'static str, value: String },
    Io(String),
    Json(String),
    Yaml(String),
    Sql(String),
    NoKv(String),
    Corpus(String),
    Judge(String),
    Http(String),
    ToolTimeout { tool: String, limit_ms: u128 },
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: DEFAULT_BUCKET.to_owned(),
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            access_key_id: DEFAULT_ACCESS_KEY.to_owned(),
            secret_access_key: DEFAULT_SECRET_KEY.to_owned(),
        }
    }
}

impl ApiSurface {
    fn parse(value: &str) -> Result<Self, HarnessError> {
        match value {
            "openai_chat_completions" => Ok(Self::ChatCompletions),
            "openai_responses" => Ok(Self::ResponsesLegacy),
            "openai_agents_responses_schema_once" => Ok(Self::AgentsResponsesSchemaOnce),
            other => Err(HarnessError::Corpus(format!(
                "unknown api_surface {other}; expected openai_chat_completions or openai_agents_responses_schema_once"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ChatCompletions => "openai_chat_completions",
            Self::ResponsesLegacy => "openai_responses",
            Self::AgentsResponsesSchemaOnce => "openai_agents_responses_schema_once",
        }
    }

    fn is_agents_schema_once(self) -> bool {
        self == Self::AgentsResponsesSchemaOnce
    }
}

impl<'de> Deserialize<'de> for ApiSurface {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

impl Serialize for ApiSurface {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl Options {
    fn parse(args: &[String]) -> Result<Self, HarnessError> {
        let mut options = Self::default();
        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--archive" => {
                    index += 1;
                    options.archive = Some(PathBuf::from(value(args, index, "--archive")?));
                }
                "--data-root" => {
                    index += 1;
                    options.data_root = Some(PathBuf::from(value(args, index, "--data-root")?));
                }
                "--db" => {
                    index += 1;
                    options.db = Some(PathBuf::from(value(args, index, "--db")?));
                }
                "--sql" => {
                    index += 1;
                    options.sql = Some(value(args, index, "--sql")?.to_owned());
                }
                "--tool-name" => {
                    index += 1;
                    options.tool_name = Some(value(args, index, "--tool-name")?.to_owned());
                }
                "--args-json" => {
                    index += 1;
                    options.args_json = Some(value(args, index, "--args-json")?.to_owned());
                }
                "--blob-ref" => {
                    index += 1;
                    options.blob_ref = Some(value(args, index, "--blob-ref")?.to_owned());
                }
                "--offset" => {
                    index += 1;
                    options.offset = Some(parse_u64(value(args, index, "--offset")?, "--offset")?);
                }
                "--limit" => {
                    index += 1;
                    options.limit = Some(parse_usize(value(args, index, "--limit")?, "--limit")?);
                }
                "--path" => {
                    index += 1;
                    options.path = Some(value(args, index, "--path")?.to_owned());
                }
                "--arm" => {
                    index += 1;
                    options.arm = Some(value(args, index, "--arm")?.to_owned());
                }
                "--task-id" => {
                    index += 1;
                    options.task_id = Some(value(args, index, "--task-id")?.to_owned());
                }
                "--output-jsonl" => {
                    index += 1;
                    options.output_jsonl =
                        Some(PathBuf::from(value(args, index, "--output-jsonl")?));
                }
                "--answer-json" => {
                    index += 1;
                    options.answer_json = Some(PathBuf::from(value(args, index, "--answer-json")?));
                }
                "--base-profile" => {
                    index += 1;
                    options.base_profile =
                        Some(PathBuf::from(value(args, index, "--base-profile")?));
                }
                "--api-surface" => {
                    index += 1;
                    options.api_surface =
                        Some(ApiSurface::parse(value(args, index, "--api-surface")?)?);
                }
                "--model" => {
                    index += 1;
                    options.model = Some(value(args, index, "--model")?.to_owned());
                }
                "--expected-generation" => {
                    index += 1;
                    options.expected_generation = Some(parse_u64(
                        value(args, index, "--expected-generation")?,
                        "--expected-generation",
                    )?);
                }
                "--max-turns" => {
                    index += 1;
                    options.max_turns = Some(parse_usize(
                        value(args, index, "--max-turns")?,
                        "--max-turns",
                    )?);
                }
                "--max-tool-calls" => {
                    index += 1;
                    options.max_tool_calls = Some(parse_usize(
                        value(args, index, "--max-tool-calls")?,
                        "--max-tool-calls",
                    )?);
                }
                "--max-completion-tokens" => {
                    index += 1;
                    options.max_completion_tokens = Some(parse_usize(
                        value(args, index, "--max-completion-tokens")?,
                        "--max-completion-tokens",
                    )?);
                }
                "--repeat" | "--repeats" => {
                    index += 1;
                    options.repeats =
                        Some(parse_usize(value(args, index, "--repeats")?, "--repeats")?);
                }
                "--run-id" => {
                    index += 1;
                    options.run_id = Some(value(args, index, "--run-id")?.to_owned());
                }
                "--reset" => options.reset = true,
                "--s3-bucket" => {
                    index += 1;
                    options.s3.bucket = value(args, index, "--s3-bucket")?.to_owned();
                }
                "--s3-endpoint" => {
                    index += 1;
                    options.s3.endpoint = value(args, index, "--s3-endpoint")?.to_owned();
                }
                "--s3-access-key-id" => {
                    index += 1;
                    options.s3.access_key_id = value(args, index, "--s3-access-key-id")?.to_owned();
                }
                "--s3-secret-access-key" => {
                    index += 1;
                    options.s3.secret_access_key =
                        value(args, index, "--s3-secret-access-key")?.to_owned();
                }
                other => return Err(HarnessError::UnknownOption(other.to_owned())),
            }
            index += 1;
        }
        Ok(options)
    }
}

impl BenchmarkRuntimeConfig {
    fn from_options(options: &Options, profile: &BaseProfile) -> Result<Self, HarnessError> {
        if profile.model.stream {
            return Err(HarnessError::Corpus(
                "base_profile.model.stream=true is not supported by this harness".to_owned(),
            ));
        }
        if !profile.run_policy.stateless {
            return Err(HarnessError::Corpus(
                "base_profile.run_policy.stateless=false is not supported by this harness"
                    .to_owned(),
            ));
        }
        if !profile.run_policy.clear_messages_after_run {
            return Err(HarnessError::Corpus(
                "base_profile.run_policy.clear_messages_after_run=false is not supported by this harness"
                    .to_owned(),
            ));
        }
        if profile.run_policy.retry_policy != "no_auto_success_retry" {
            return Err(HarnessError::Corpus(format!(
                "base_profile.run_policy.retry_policy={} is not supported by this harness",
                profile.run_policy.retry_policy
            )));
        }
        if profile.run_policy.corpus_snapshot != "fixed_per_benchmark_batch" {
            return Err(HarnessError::Corpus(format!(
                "base_profile.run_policy.corpus_snapshot={} is not supported by this harness",
                profile.run_policy.corpus_snapshot
            )));
        }
        let model = options
            .model
            .clone()
            .ok_or_else(|| HarnessError::Corpus("--model is required".to_owned()))?;
        let max_turns = options.max_turns.unwrap_or(profile.run_policy.max_turns);
        let max_tool_calls = options
            .max_tool_calls
            .unwrap_or(profile.run_policy.max_tool_calls);
        let max_completion_tokens = options
            .max_completion_tokens
            .unwrap_or(profile.model.max_completion_tokens);
        let response_format = profile
            .model
            .structured_output
            .then(|| json!({"type": "json_object"}));
        Ok(Self {
            model,
            api_surface: options.api_surface.unwrap_or(profile.api_surface),
            max_turns,
            max_tool_calls,
            max_completion_tokens,
            tool_call_timeout: Duration::from_millis(profile.run_policy.tool_call_timeout_ms),
            temperature: profile.model.temperature,
            response_format,
        })
    }
}

#[cfg(test)]
fn base_profile() -> Result<LoadedBaseProfile, HarnessError> {
    load_base_profile_path(&default_base_profile_path())
}

fn load_base_profile(options: &Options) -> Result<LoadedBaseProfile, HarnessError> {
    let path = options
        .base_profile
        .clone()
        .unwrap_or_else(default_base_profile_path);
    load_base_profile_path(&path)
}

fn load_base_profile_path(path: &Path) -> Result<LoadedBaseProfile, HarnessError> {
    let yaml = fs::read_to_string(path).map_err(from_io)?;
    let profile = parse_base_profile_yaml(&yaml)?;
    Ok(LoadedBaseProfile {
        profile,
        #[cfg(test)]
        yaml,
    })
}

fn parse_base_profile_yaml(yaml: &str) -> Result<BaseProfile, HarnessError> {
    serde_yaml::from_str(yaml).map_err(|err| HarnessError::Yaml(err.to_string()))
}

fn default_base_profile_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agent-interface/base_profile.yaml")
}

fn repeat_count_from_options(options: &Options, profile: &BaseProfile) -> usize {
    options
        .repeats
        .unwrap_or(profile.run_policy.repeats_per_arm_task)
}

fn prepare(options: Options) -> Result<(), HarnessError> {
    let archive = options
        .archive
        .as_ref()
        .ok_or(HarnessError::MissingOption("--archive"))?;
    let data_root = options
        .data_root
        .as_ref()
        .ok_or(HarnessError::MissingOption("--data-root"))?;
    let corpus_root = data_root.join("corpus");
    let sqlite_path = sqlite_path(data_root);
    let nokv_meta_path = nokv_meta_path(data_root);

    if options.reset {
        remove_if_exists(&corpus_root)?;
        remove_if_exists(&sqlite_path)?;
        remove_if_exists(&nokv_meta_path)?;
    }
    fs::create_dir_all(data_root).map_err(from_io)?;
    if !corpus_root.exists() {
        extract_archive(archive, &corpus_root)?;
    }

    let runs = load_runs(&corpus_root)?;
    let indexes = derive_indexes(&runs)?;
    prepare_sqlite(&sqlite_path, &runs, &indexes)?;
    prepare_nokv(&nokv_meta_path, &options.s3, &runs, &indexes)?;

    let manifest = PrepareManifest {
        archive_sha256: file_sha256(archive)?,
        run_count: runs.len(),
        metadata_files: runs.len(),
        metrics_files: runs
            .iter()
            .filter(|run| run.metrics_bytes.is_some())
            .count(),
        params_files: runs.iter().filter(|run| run.params_bytes.is_some()).count(),
        dependencies_files: runs
            .iter()
            .filter(|run| run.dependencies_bytes.is_some())
            .count(),
        artifact_files: runs.iter().map(|run| run.artifacts.len()).sum(),
        missing_artifact_refs: runs.iter().map(missing_artifact_ref_count).sum(),
        sqlite_db: sqlite_path.display().to_string(),
        nokv_meta: nokv_meta_path.display().to_string(),
        status_counts: indexes.status_counts,
        tag_counts: indexes.tag_counts,
        script_counts: indexes.script_counts,
    };
    fs::write(
        data_root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest serializes"),
    )
    .map_err(from_io)?;
    println!(
        "prepared run_count={} sqlite={} nokv_meta={}",
        manifest.run_count, manifest.sqlite_db, manifest.nokv_meta
    );
    Ok(())
}

fn verify(options: Options) -> Result<VerificationReport, HarnessError> {
    let data_root = options
        .data_root
        .as_ref()
        .ok_or(HarnessError::MissingOption("--data-root"))?;
    let db = Connection::open_with_flags(
        sqlite_path(data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let run_id_sample = match options.run_id {
        Some(run_id) => run_id,
        None => first_experiment_id(&db)?,
    };
    let sqlite = verify_sqlite_materialization(&db)?;
    let service = open_existing_nokv(&nokv_meta_path(data_root), &options.s3)?;
    let nokv_native = verify_nokv_namespace(&db, &service)?;
    let all_available_reads_match =
        sqlite.mismatches.is_empty() && nokv_native.mismatches.is_empty();
    Ok(VerificationReport {
        run_id_sample,
        sqlite,
        nokv_native,
        all_available_reads_match,
    })
}

fn show_schema(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    print!("{}", sqlite_schema_text(&conn)?);
    Ok(())
}

fn query_sql(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let sql = options.sql.ok_or(HarnessError::MissingOption("--sql"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    let result = execute_sqlite_query_tool(&conn, &sql)?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    Ok(())
}

fn execute_sqlite_query_tool(
    conn: &Connection,
    sql: &str,
) -> Result<SqliteQueryToolResult, HarnessError> {
    execute_sqlite_query_tool_with_timeout(conn, sql, Duration::from_millis(TOOL_CALL_TIMEOUT_MS))
}

fn execute_sqlite_query_tool_with_timeout(
    conn: &Connection,
    sql: &str,
    timeout: Duration,
) -> Result<SqliteQueryToolResult, HarnessError> {
    let trimmed = sql.trim_start().to_ascii_lowercase();
    if !(trimmed.starts_with("select")
        || trimmed.starts_with("with")
        || trimmed.starts_with("explain"))
    {
        return Err(HarnessError::Sql(
            "sqlite-query only allows SELECT, WITH, and EXPLAIN statements".to_owned(),
        ));
    }
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_for_handler = Arc::clone(&timed_out);
    let started = Instant::now();
    conn.progress_handler(
        SQLITE_PROGRESS_OPS,
        Some(move || {
            let should_interrupt = started.elapsed() >= timeout;
            if should_interrupt {
                timed_out_for_handler.store(true, Ordering::Relaxed);
            }
            should_interrupt
        }),
    );
    let _progress_guard = SqliteProgressGuard { conn };

    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
    let names = stmt
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map([], |row| {
            let mut out = serde_json::Map::new();
            for (index, name) in names.iter().enumerate() {
                let value = sqlite_value(row, index)?;
                out.insert(name.clone(), value);
            }
            Ok(Value::Object(out))
        })
        .map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
    let mut tool_rows = Vec::new();
    let row_limit = 1000;
    let mut truncated = false;
    for row in rows.take(row_limit + 1) {
        let row = row.map_err(|err| sqlite_timeout_or_error(err, &timed_out, timeout))?;
        if tool_rows.len() == row_limit {
            truncated = true;
            break;
        }
        tool_rows.push(SqliteQueryToolRow {
            evidence: evidence_refs_for_row(&row),
            row,
        });
    }
    Ok(SqliteQueryToolResult {
        tool: "query_sql",
        row_limit,
        truncated,
        rows: tool_rows,
    })
}

struct SqliteProgressGuard<'conn> {
    conn: &'conn Connection,
}

impl Drop for SqliteProgressGuard<'_> {
    fn drop(&mut self) {
        self.conn.progress_handler(0, None::<fn() -> bool>);
    }
}

fn sqlite_timeout_or_error(
    err: impl Error,
    timed_out: &AtomicBool,
    timeout: Duration,
) -> HarnessError {
    if timed_out.load(Ordering::Relaxed) {
        HarnessError::ToolTimeout {
            tool: "query_sql".to_owned(),
            limit_ms: duration_millis(timeout),
        }
    } else {
        from_sql(err)
    }
}

fn read_blob(options: Options) -> Result<(), HarnessError> {
    let db = options.db.ok_or(HarnessError::MissingOption("--db"))?;
    let blob_ref = options
        .blob_ref
        .ok_or(HarnessError::MissingOption("--blob-ref"))?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    let conn = Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(from_sql)?;
    let result = read_blob_tool(&conn, &blob_ref, offset, limit)?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap());
    Ok(())
}

fn read_blob_tool(
    conn: &Connection,
    blob_ref: &str,
    offset: u64,
    limit: usize,
) -> Result<SqliteReadBlobToolResult, HarnessError> {
    let (size_bytes, digest, content): (i64, String, Vec<u8>) = conn
        .query_row(
            "SELECT size_bytes, digest, content FROM blobs WHERE blob_ref = ?1",
            params![blob_ref],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(from_sql)?;
    let total_size_bytes = usize::try_from(size_bytes).map_err(|_| {
        HarnessError::Sql(format!(
            "blob {blob_ref} has negative or oversized size {size_bytes}"
        ))
    })?;
    let start = usize::try_from(offset).map_err(|_| HarnessError::InvalidNumber {
        option: "--offset",
        value: offset.to_string(),
    })?;
    let end = start.saturating_add(limit).min(content.len());
    let range = if start >= content.len() {
        Vec::new()
    } else {
        content[start..end].to_vec()
    };
    Ok(SqliteReadBlobToolResult {
        tool: "read_blob",
        blob_ref: blob_ref.to_owned(),
        evidence: format!("sqlite://blobs/{blob_ref}"),
        total_size_bytes,
        digest,
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
    })
}

fn nokv_list(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    print_json(&nokv_list_tool(&service, &path)?)
}

fn nokv_stat(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    print_json(&nokv_stat_tool(&service, &path)?)
}

fn nokv_read(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    let path = required_path(&options)?;
    let offset = options.offset.unwrap_or(0);
    let limit = options
        .limit
        .ok_or(HarnessError::MissingOption("--limit"))?;
    print_json(&nokv_read_tool(
        &service,
        &path,
        offset,
        limit,
        options.expected_generation,
    )?)
}

fn nokv_list_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
) -> Result<ListToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let mut entries = service.read_dir_plus_path(&path).map_err(from_nokv)?;
    entries.sort_by(|left, right| {
        left.dentry
            .name
            .as_bytes()
            .cmp(right.dentry.name.as_bytes())
    });
    let row_limit = 1000;
    let truncated = entries.len() > row_limit;
    let entries = entries
        .into_iter()
        .take(row_limit)
        .map(|entry| {
            let name = String::from_utf8_lossy(entry.dentry.name.as_bytes()).to_string();
            let child_path = join_absolute_path(&path, &name);
            nokv_entry(&child_path, &entry.attr, entry.body.as_ref())
        })
        .collect();
    Ok(ListToolResult {
        tool: "list",
        evidence: format!("nokv-native://{path}"),
        path,
        entries,
        row_limit,
        truncated,
    })
}

fn nokv_stat_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
) -> Result<StatToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let metadata = service
        .stat_path(&path)
        .map_err(from_nokv)?
        .ok_or_else(|| HarnessError::NoKv(format!("{path}: path not found")))?;
    Ok(nokv_stat_result(
        &path,
        &metadata.attr,
        metadata.body.as_ref(),
    ))
}

fn nokv_read_tool(
    service: &NoKvFs<HoltMetadataStore, S3ObjectStore>,
    path: &str,
    offset: u64,
    limit: usize,
    expected_generation: Option<u64>,
) -> Result<ReadToolResult, HarnessError> {
    let path = normalize_absolute_path(path);
    let metadata = service
        .stat_path(&path)
        .map_err(from_nokv)?
        .ok_or_else(|| HarnessError::NoKv(format!("{path}: path not found")))?;
    if metadata.attr.file_type != FileType::File {
        return Err(HarnessError::NoKv(format!("{path}: path is not a file")));
    }
    if let Some(expected) = expected_generation {
        if expected != metadata.attr.generation {
            return Ok(ReadToolResult {
                tool: "read",
                evidence: format!(
                    "nokv-native://{path}@generation:{}",
                    metadata.attr.generation
                ),
                path,
                total_size_bytes: metadata.attr.size,
                digest: metadata.body.as_ref().map(|body| body.digest_uri.clone()),
                offset,
                requested_limit: limit,
                bytes_read: 0,
                content_utf8: None,
                content_hex: String::new(),
                generation: Some(metadata.attr.generation),
                body: metadata.body.as_ref().map(body_summary),
                generation_mismatch: Some(GenerationMismatch {
                    expected,
                    current: metadata.attr.generation,
                }),
            });
        }
    }
    let range = service
        .read_file(metadata.attr.inode, offset, limit)
        .map_err(from_nokv)?;
    Ok(ReadToolResult {
        tool: "read",
        evidence: format!(
            "nokv-native://{path}@generation:{}",
            metadata.attr.generation
        ),
        path,
        total_size_bytes: metadata.attr.size,
        digest: metadata.body.as_ref().map(|body| body.digest_uri.clone()),
        offset,
        requested_limit: limit,
        bytes_read: range.len(),
        content_utf8: std::str::from_utf8(&range).ok().map(str::to_owned),
        content_hex: bytes_hex(&range),
        generation: Some(metadata.attr.generation),
        body: metadata.body.as_ref().map(body_summary),
        generation_mismatch: None,
    })
}

fn required_data_root(options: &Options) -> Result<PathBuf, HarnessError> {
    options
        .data_root
        .clone()
        .ok_or(HarnessError::MissingOption("--data-root"))
}

fn required_path(options: &Options) -> Result<String, HarnessError> {
    options
        .path
        .clone()
        .ok_or(HarnessError::MissingOption("--path"))
}

fn print_json(value: &impl Serialize) -> Result<(), HarnessError> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("tool result serializes")
    );
    Ok(())
}

fn normalize_absolute_path(path: &str) -> String {
    let mut out = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    };
    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }
    out
}

fn path_name(path: &str) -> &str {
    path.rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("/")
}

fn join_absolute_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}

fn nokv_entry(path: &str, attr: &InodeAttr, body: Option<&BodyDescriptor>) -> FileEntry {
    FileEntry {
        name: path_name(path).to_owned(),
        path: path.to_owned(),
        file_type: file_type_name(attr.file_type).to_owned(),
        size_bytes: (attr.file_type == FileType::File).then_some(attr.size),
        digest: body.map(|body| body.digest_uri.clone()),
        evidence: format!("nokv-native://{path}@generation:{}", attr.generation),
        inode: Some(attr.inode.get()),
        mode: Some(attr.mode),
        uid: Some(attr.uid),
        gid: Some(attr.gid),
        modified_ms: i64::try_from(attr.mtime_ms).ok(),
        generation: Some(attr.generation),
        body: body.map(body_summary),
    }
}

fn nokv_stat_result(path: &str, attr: &InodeAttr, body: Option<&BodyDescriptor>) -> StatToolResult {
    StatToolResult {
        tool: "stat",
        path: path.to_owned(),
        evidence: format!("nokv-native://{path}@generation:{}", attr.generation),
        file_type: file_type_name(attr.file_type).to_owned(),
        size_bytes: (attr.file_type == FileType::File).then_some(attr.size),
        digest: body.map(|body| body.digest_uri.clone()),
        inode: Some(attr.inode.get()),
        mode: Some(attr.mode),
        uid: Some(attr.uid),
        gid: Some(attr.gid),
        modified_ms: i64::try_from(attr.mtime_ms).ok(),
        generation: Some(attr.generation),
        body: body.map(body_summary),
    }
}

fn body_summary(body: &BodyDescriptor) -> BodyDescriptorSummary {
    BodyDescriptorSummary {
        producer: body.producer.clone(),
        digest_uri: body.digest_uri.clone(),
        size: body.size,
        content_type: body.content_type.clone(),
        manifest_id: body.manifest_id.clone(),
        generation: body.generation,
        chunk_size: body.chunk_size,
        block_size: body.block_size,
    }
}

fn file_type_name(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
        FileType::NamedPipe | FileType::CharDevice | FileType::BlockDevice | FileType::Socket => {
            "special"
        }
    }
}

fn print_tool_registry(options: Options) -> Result<(), HarnessError> {
    let arm = options.arm.ok_or(HarnessError::MissingOption("--arm"))?;
    print_json(&tool_registry_for_arm(&arm)?)
}

fn tool_registry_for_arm(arm: &str) -> Result<Vec<ToolDefinition>, HarnessError> {
    if arm == "nokv_native_v1" {
        let tools = agent_tool_definitions()
            .into_iter()
            .map(|tool| ToolDefinition {
                name: tool.name.to_owned(),
                description: tool.description.to_owned(),
                parameters: tool.parameters,
            })
            .collect::<Vec<_>>();
        return Ok(tools);
    }
    let names = match arm {
        "sqlite_raw_v1" => vec![
            (
                "show_schema",
                "Return the live SQLite schema for tables and indexes. Use this before non-trivial SQL and rely only on discovered table and column names.",
                json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                }),
            ),
            (
                "query_sql",
                "Execute bounded read-only SQLite. Only SELECT, WITH, and EXPLAIN statements are accepted; mutations and pragmas are rejected.",
                json!({
                    "type": "object",
                    "required": ["sql"],
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "A bounded read-only SELECT, WITH, or EXPLAIN statement over schema-discovered tables and columns."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
            (
                "read_blob",
                "Read a byte range from a blob_ref returned by query_sql. Use read_blob only when body bytes are required; do not read blobs when metadata rows already answer the task.",
                json!({
                    "type": "object",
                    "required": ["blob_ref", "offset", "limit"],
                    "properties": {
                        "blob_ref": {
                            "type": "string",
                            "description": "Blob reference returned by query_sql."
                        },
                        "offset": {
                            "type": "integer",
                            "minimum": 0,
                            "description": "Zero-based byte offset."
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 65536,
                            "description": "Maximum bytes to return from this blob range."
                        }
                    },
                    "additionalProperties": false
                }),
            ),
            (
                "grep_blob",
                "Search a blob's text for a case-insensitive literal substring and return matching lines with line numbers. Prefer grep_blob over read_blob when only matching lines are needed.",
                json!({
                    "type": "object",
                    "required": ["blob_ref", "pattern"],
                    "properties": {
                        "blob_ref": {
                            "type": "string",
                            "description": "Blob reference returned by query_sql."
                        },
                        "pattern": {
                            "type": "string",
                            "description": "Case-insensitive literal substring to match against each text line."
                        },
                        "cursor": {"type": ["string", "null"]},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100}
                    },
                    "additionalProperties": false
                }),
            ),
        ],
        other => {
            return Err(HarnessError::Corpus(format!(
                "unknown benchmark arm {other}"
            )))
        }
    };
    Ok(names
        .into_iter()
        .map(|(name, description, parameters)| ToolDefinition {
            name: name.to_owned(),
            description: description.to_owned(),
            parameters,
        })
        .collect())
}

fn phase1_task_set() -> Result<BenchmarkTaskSet, HarnessError> {
    serde_yaml::from_str(include_str!(
        "../../agent-interface/tasks/phase1_readonly.yaml"
    ))
    .map_err(|err| HarnessError::Yaml(err.to_string()))
}

fn benchmark_arm_ids() -> &'static [&'static str] {
    &["sqlite_raw_v1", "nokv_native_v1"]
}

fn batch_plan(
    arm: Option<&str>,
    task_id: Option<&str>,
    repeat_count: usize,
    task_set: &BenchmarkTaskSet,
) -> Vec<BatchPlanItem> {
    let arms = arm.map(|arm| vec![arm.to_owned()]).unwrap_or_else(|| {
        benchmark_arm_ids()
            .iter()
            .map(|arm| (*arm).to_owned())
            .collect()
    });
    let tasks = task_set
        .tasks
        .iter()
        .filter(|task| task_id.map(|id| id == task.task_id).unwrap_or(true))
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let mut plan = Vec::new();
    for arm_id in arms {
        for task_id in &tasks {
            for repeat_index in 0..repeat_count {
                plan.push(BatchPlanItem {
                    arm_id: arm_id.clone(),
                    task_id: task_id.clone(),
                    repeat_index,
                });
            }
        }
    }
    plan
}

fn list_tasks() -> Result<(), HarnessError> {
    print_json(&phase1_task_set()?)
}

fn show_task(options: Options) -> Result<(), HarnessError> {
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let task = find_task(&task_id)?;
    print_json(&task)
}

fn find_task(task_id: &str) -> Result<BenchmarkTask, HarnessError> {
    phase1_task_set()?
        .tasks
        .into_iter()
        .find(|task| task.task_id == task_id)
        .ok_or_else(|| HarnessError::Corpus(format!("unknown task_id {task_id}")))
}

fn print_gold(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let task = find_task(&task_id)?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let rows = query_gold_rows(&conn, &task)?;
    print_json(&json!({
        "task_id": task.task_id,
        "gold_rows": rows,
    }))
}

fn judge(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let task_id = options
        .task_id
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let arm = options.arm.ok_or(HarnessError::MissingOption("--arm"))?;
    let answer_json = options
        .answer_json
        .ok_or(HarnessError::MissingOption("--answer-json"))?;
    let task = find_task(&task_id)?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let answer = serde_json::from_slice(&fs::read(answer_json).map_err(from_io)?)
        .map_err(|err| HarnessError::Json(err.to_string()))?;
    let nokv_for_evidence = if arm == "nokv_native_v1" {
        Some(open_existing_nokv(
            &nokv_meta_path(&data_root),
            &options.s3,
        )?)
    } else {
        None
    };
    print_json(&judge_answer(
        &conn,
        &task,
        &arm,
        &answer,
        nokv_for_evidence.as_ref(),
    )?)
}

fn run_benchmark_task(options: Options) -> Result<(), HarnessError> {
    let arm = options
        .arm
        .as_deref()
        .ok_or(HarnessError::MissingOption("--arm"))?;
    let task_id = options
        .task_id
        .as_deref()
        .ok_or(HarnessError::MissingOption("--task-id"))?;
    let profile = load_base_profile(&options)?;
    let final_answer = run_benchmark_task_once(&options, &profile, arm, task_id, 0)?;
    print_json(&final_answer)
}

fn run_benchmark_batch(options: Options) -> Result<(), HarnessError> {
    let _ = required_data_root(&options)?;
    let _ = options
        .output_jsonl
        .as_ref()
        .ok_or(HarnessError::MissingOption("--output-jsonl"))?;
    let task_set = phase1_task_set()?;
    let profile = load_base_profile(&options)?;
    let repeat_count = repeat_count_from_options(&options, &profile);
    let plan = batch_plan(
        options.arm.as_deref(),
        options.task_id.as_deref(),
        repeat_count,
        &task_set,
    );
    if plan.is_empty() {
        return Err(HarnessError::Corpus(
            "batch plan is empty; check --arm, --task-id, and --repeats".to_owned(),
        ));
    }
    for item in &plan {
        run_benchmark_task_once(
            &options,
            &profile,
            &item.arm_id,
            &item.task_id,
            item.repeat_index,
        )?;
    }
    print_json(&json!({
        "status": "completed",
        "runs": plan.len(),
        "repeats": repeat_count,
        "output_jsonl": options.output_jsonl.as_ref().map(|path| path.display().to_string()),
    }))
}

fn run_benchmark_task_once(
    options: &Options,
    profile: &LoadedBaseProfile,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
) -> Result<Value, HarnessError> {
    let api_surface = options.api_surface.unwrap_or(profile.api_surface);
    if api_surface.is_agents_schema_once() {
        return run_benchmark_task_once_agents_schema_once(
            options,
            profile,
            arm,
            task_id,
            repeat_index,
        );
    }
    run_benchmark_task_once_legacy(options, profile, arm, task_id, repeat_index)
}

fn run_benchmark_task_once_legacy(
    options: &Options,
    profile: &LoadedBaseProfile,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
) -> Result<Value, HarnessError> {
    let data_root = required_data_root(options)?;
    let output_jsonl = options
        .output_jsonl
        .clone()
        .ok_or(HarnessError::MissingOption("--output-jsonl"))?;
    let task = find_task(task_id)?;
    let tools = tool_registry_for_arm(arm)?;
    let started = now_unix_ms();
    let run_id = format!("{}-{}-r{}-{started}", arm, task.task_id, repeat_index);
    let runtime_config = BenchmarkRuntimeConfig::from_options(options, profile)?;
    let model = runtime_config.model.clone();
    let api_key =
        env::var("OPENAI_API_KEY").map_err(|_| HarnessError::MissingOption("OPENAI_API_KEY"))?;
    let endpoint = env::var("OPENAI_RESPONSES_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1/responses".to_owned());
    let arm_card = arm_card_yaml(arm)?;
    let initial_messages = benchmark_messages(profile, arm_card, &task);
    let response_tools = response_tools(&tools);
    let client = reqwest::blocking::Client::new();
    let tool_worker = ToolWorker::start(
        arm,
        &data_root,
        &options.s3,
        &tools,
        runtime_config.tool_call_timeout,
    );
    let mut api_calls = Vec::new();
    let mut tool_calls = Vec::new();
    let mut invalid_sql_count = 0;
    let mut wrong_tool_count = 0;
    let mut tool_timeout_count = 0;
    let mut tool_bytes_read = 0;
    let mut tool_result_tokens = 0;
    let mut final_answer = None;
    let mut previous_response_id: Option<String> = None;
    let mut pending_input = response_input_messages(&initial_messages);

    for _turn in 0..runtime_config.max_turns {
        let first_model_request = previous_response_id.is_none();
        let request = ResponseRequest {
            model: model.clone(),
            input: pending_input.clone(),
            tools: first_model_request.then(|| response_tools.clone()),
            previous_response_id: previous_response_id.clone(),
            temperature: runtime_config.temperature,
            max_output_tokens: runtime_config.max_completion_tokens,
            text: first_model_request
                .then(|| response_text_config(runtime_config.response_format.clone()))
                .flatten(),
        };
        let api_started = now_unix_ms();
        let response = match send_model_response(&client, &endpoint, &api_key, &request) {
            Ok(response) => response,
            Err(err) if is_recoverable_run_api_error(&err) => {
                let error = err.to_string();
                let final_answer =
                    run_error_answer(&task, &error, tool_calls.len(), tool_bytes_read);
                let completed = now_unix_ms();
                let mut derived = DerivedMetrics {
                    interface_card_tokens: estimate_tokens(arm_card),
                    task_prompt_tokens: estimate_tokens(&task.prompt),
                    wall_time_ms: completed.saturating_sub(started),
                    task_success: Some(false),
                    evidence_precision: None,
                    tool_call_count: tool_calls.len(),
                    tool_result_tokens,
                    tool_bytes_read,
                    invalid_sql_count,
                    wrong_tool_count,
                    tool_timeout_count,
                    missing_evidence_count: final_answer
                        .get("missing_evidence")
                        .and_then(Value::as_array)
                        .map(Vec::len)
                        .unwrap_or_default(),
                    overread_bytes: 0,
                    ..DerivedMetrics::default()
                };
                apply_usage_costs(&mut derived, &api_calls, &model);
                let telemetry = BenchmarkRunTelemetry {
                    record_type: "benchmark_run".to_owned(),
                    run_id,
                    arm_id: arm.to_owned(),
                    task_id: task.task_id,
                    repeat_index,
                    model,
                    started_at_unix_ms: started,
                    completed_at_unix_ms: completed,
                    api_calls,
                    tool_calls,
                    derived_metrics: derived,
                    correctness: false,
                    judge: None,
                    final_answer: Some(final_answer.clone()),
                    run_error: Some(error),
                };
                append_jsonl(&output_jsonl, &telemetry)?;
                return Ok(final_answer);
            }
            Err(err) => return Err(err),
        };
        let api_completed = now_unix_ms();
        api_calls.push(api_call_telemetry(
            &response,
            &model,
            api_started,
            api_completed,
            request.previous_response_id.clone(),
            request.tools.is_some(),
            first_model_request,
        ));
        let calls = response_tool_calls(&response)?;
        if !calls.is_empty() {
            if tool_calls.len() + calls.len() > runtime_config.max_tool_calls {
                return Err(HarnessError::Http(format!(
                    "tool call budget exceeded: max_tool_calls={}",
                    runtime_config.max_tool_calls
                )));
            }
            let response_id = response.id.clone().ok_or_else(|| {
                HarnessError::Http(
                    "OpenAI response with tool calls did not include a continuation id".to_owned(),
                )
            })?;
            previous_response_id = Some(response_id);
            let mut tool_output_input = Vec::new();
            for call in calls {
                let tool_started = now_unix_ms();
                let arguments = tool_call_arguments(&call);
                append_tool_call_start_jsonl(
                    &output_jsonl,
                    &run_id,
                    arm,
                    &task.task_id,
                    repeat_index,
                    &call,
                    tool_started,
                )?;
                let outcome = tool_worker.execute_tool_call(&call);
                let tool_completed = now_unix_ms();
                invalid_sql_count += usize::from(outcome.invalid_sql);
                wrong_tool_count += usize::from(outcome.wrong_tool);
                tool_timeout_count += usize::from(outcome.timed_out);
                tool_bytes_read += outcome.bytes_read;
                tool_result_tokens += outcome.result_token_estimate;
                tool_calls.push(ToolCallTelemetry {
                    call_id: call.id.clone(),
                    tool_name: call.function.name.clone(),
                    arguments,
                    started_at_unix_ms: tool_started,
                    completed_at_unix_ms: tool_completed,
                    status: outcome.status.clone(),
                    bytes_read: outcome.bytes_read,
                    result_token_estimate: outcome.result_token_estimate,
                    error: outcome.error.clone(),
                    timed_out: outcome.timed_out,
                });
                if outcome.fatal_run_error {
                    let error = outcome.error.clone().unwrap_or_else(|| {
                        format!("tool {} failed with a fatal run error", call.function.name)
                    });
                    let final_answer =
                        run_error_answer(&task, &error, tool_calls.len(), tool_bytes_read);
                    let completed = now_unix_ms();
                    let mut derived = DerivedMetrics {
                        interface_card_tokens: estimate_tokens(arm_card),
                        task_prompt_tokens: estimate_tokens(&task.prompt),
                        wall_time_ms: completed.saturating_sub(started),
                        task_success: Some(false),
                        evidence_precision: None,
                        tool_call_count: tool_calls.len(),
                        tool_result_tokens,
                        tool_bytes_read,
                        invalid_sql_count,
                        wrong_tool_count,
                        tool_timeout_count,
                        missing_evidence_count: final_answer
                            .get("missing_evidence")
                            .and_then(Value::as_array)
                            .map(Vec::len)
                            .unwrap_or_default(),
                        overread_bytes: 0,
                        ..DerivedMetrics::default()
                    };
                    apply_usage_costs(&mut derived, &api_calls, &model);
                    let telemetry = BenchmarkRunTelemetry {
                        record_type: "benchmark_run".to_owned(),
                        run_id,
                        arm_id: arm.to_owned(),
                        task_id: task.task_id,
                        repeat_index,
                        model,
                        started_at_unix_ms: started,
                        completed_at_unix_ms: completed,
                        api_calls,
                        tool_calls,
                        derived_metrics: derived,
                        correctness: false,
                        judge: None,
                        final_answer: Some(final_answer.clone()),
                        run_error: Some(error),
                    };
                    append_jsonl(&output_jsonl, &telemetry)?;
                    return Ok(final_answer);
                }
                tool_output_input.push(response_function_call_output(&call.id, &outcome.content));
            }
            pending_input = tool_output_input;
            continue;
        }
        if let Some(content) = response_final_text(&response) {
            let parsed = serde_json::from_str::<Value>(&content)
                .map_err(|err| HarnessError::Json(format!("final answer is not JSON: {err}")))?;
            final_answer = Some(parsed);
            break;
        }
    }

    let final_answer = final_answer.ok_or_else(|| {
        HarnessError::Http("model did not produce a final JSON answer before max_turns".to_owned())
    })?;
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let completed = now_unix_ms();
    let missing_evidence_count = final_answer
        .get("missing_evidence")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let judge_result = match judge_answer(&conn, &task, arm, &final_answer, None) {
        Ok(judge_result) => judge_result,
        Err(err) if is_recoverable_run_judge_error(&err) => {
            let error = err.to_string();
            let mut derived = DerivedMetrics {
                interface_card_tokens: estimate_tokens(arm_card),
                task_prompt_tokens: estimate_tokens(&task.prompt),
                wall_time_ms: completed.saturating_sub(started),
                task_success: Some(false),
                evidence_precision: None,
                tool_call_count: tool_calls.len(),
                tool_result_tokens,
                tool_bytes_read,
                invalid_sql_count,
                wrong_tool_count,
                tool_timeout_count,
                missing_evidence_count,
                overread_bytes: 0,
                ..DerivedMetrics::default()
            };
            apply_usage_costs(&mut derived, &api_calls, &model);
            let telemetry = BenchmarkRunTelemetry {
                record_type: "benchmark_run".to_owned(),
                run_id,
                arm_id: arm.to_owned(),
                task_id: task.task_id,
                repeat_index,
                model,
                started_at_unix_ms: started,
                completed_at_unix_ms: completed,
                api_calls,
                tool_calls,
                derived_metrics: derived,
                correctness: false,
                judge: None,
                final_answer: Some(final_answer.clone()),
                run_error: Some(error),
            };
            append_jsonl(&output_jsonl, &telemetry)?;
            return Ok(final_answer);
        }
        Err(err) => return Err(err),
    };
    let mut derived = DerivedMetrics {
        interface_card_tokens: estimate_tokens(arm_card),
        task_prompt_tokens: estimate_tokens(&task.prompt),
        wall_time_ms: completed.saturating_sub(started),
        task_success: Some(judge_result.task_success),
        evidence_precision: judge_result.evidence_precision,
        tool_call_count: tool_calls.len(),
        tool_result_tokens,
        tool_bytes_read,
        invalid_sql_count,
        wrong_tool_count,
        tool_timeout_count,
        missing_evidence_count,
        overread_bytes: 0,
        ..DerivedMetrics::default()
    };
    apply_usage_costs(&mut derived, &api_calls, &model);
    let telemetry = BenchmarkRunTelemetry {
        record_type: "benchmark_run".to_owned(),
        run_id,
        arm_id: arm.to_owned(),
        task_id: task.task_id,
        repeat_index,
        model,
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        api_calls,
        tool_calls,
        derived_metrics: derived,
        correctness: judge_result.task_success,
        judge: Some(judge_result),
        final_answer: Some(final_answer.clone()),
        run_error: None,
    };
    append_jsonl(&output_jsonl, &telemetry)?;
    Ok(final_answer)
}

fn run_benchmark_task_once_agents_schema_once(
    options: &Options,
    profile: &LoadedBaseProfile,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
) -> Result<Value, HarnessError> {
    let data_root = required_data_root(options)?;
    let output_jsonl = options
        .output_jsonl
        .clone()
        .ok_or(HarnessError::MissingOption("--output-jsonl"))?;
    let task = find_task(task_id)?;
    let tools = tool_registry_for_arm(arm)?;
    let started = now_unix_ms();
    let run_id = format!("{}-{}-r{}-{started}", arm, task.task_id, repeat_index);
    let runtime_config = BenchmarkRuntimeConfig::from_options(options, profile)?;
    let model = runtime_config.model.clone();
    let _api_key =
        env::var("OPENAI_API_KEY").map_err(|_| HarnessError::MissingOption("OPENAI_API_KEY"))?;
    let endpoint = env::var("OPENAI_RESPONSES_URL")
        .unwrap_or_else(|_| "https://api.openai.com/v1/responses".to_owned());
    let arm_card = arm_card_yaml(arm)?;
    let messages = benchmark_messages(profile, arm_card, &task);
    let bridge = ToolBridgeServer::start(ToolBridgeStartConfig {
        run_id: &run_id,
        arm,
        task_id: &task.task_id,
        repeat_index,
        data_root: &data_root,
        s3: &options.s3,
        registry: &tools,
        timeout: runtime_config.tool_call_timeout,
        output_jsonl: output_jsonl.clone(),
    })?;
    let runner_config = AgentRunnerConfig {
        run_id: run_id.clone(),
        model: model.clone(),
        endpoint,
        arm: arm.to_owned(),
        task_id: task.task_id.clone(),
        max_turns: runtime_config.max_turns,
        max_completion_tokens: runtime_config.max_completion_tokens,
        temperature: runtime_config.temperature,
        response_format: runtime_config.response_format.clone(),
        messages,
        tool_bridge_url: bridge.url().to_owned(),
        tools,
    };
    let runner_output = run_python_agent_runner(&runner_config);
    let bridge_snapshot = bridge.finish()?;
    let runner_output = runner_output?;

    let api_calls = runner_output.api_calls;
    let completed = now_unix_ms();
    let run_error = runner_output
        .run_error
        .or_else(|| bridge_snapshot.fatal_error.clone());
    if let Some(error) = run_error {
        let final_answer = run_error_answer(
            &task,
            &error,
            bridge_snapshot.tool_call_count(),
            bridge_snapshot.tool_bytes_read,
        );
        let mut derived = DerivedMetrics {
            interface_card_tokens: estimate_tokens(arm_card),
            task_prompt_tokens: estimate_tokens(&task.prompt),
            wall_time_ms: completed.saturating_sub(started),
            task_success: Some(false),
            evidence_precision: None,
            tool_call_count: bridge_snapshot.tool_call_count(),
            tool_result_tokens: bridge_snapshot.tool_result_tokens,
            tool_bytes_read: bridge_snapshot.tool_bytes_read,
            invalid_sql_count: bridge_snapshot.invalid_sql_count,
            wrong_tool_count: bridge_snapshot.wrong_tool_count,
            tool_timeout_count: bridge_snapshot.tool_timeout_count,
            missing_evidence_count: final_answer
                .get("missing_evidence")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default(),
            overread_bytes: 0,
            ..DerivedMetrics::default()
        };
        apply_usage_costs(&mut derived, &api_calls, &model);
        let telemetry = BenchmarkRunTelemetry {
            record_type: "benchmark_run".to_owned(),
            run_id,
            arm_id: arm.to_owned(),
            task_id: task.task_id,
            repeat_index,
            model,
            started_at_unix_ms: started,
            completed_at_unix_ms: completed,
            api_calls,
            tool_calls: bridge_snapshot.tool_calls,
            derived_metrics: derived,
            correctness: false,
            judge: None,
            final_answer: Some(final_answer.clone()),
            run_error: Some(error),
        };
        append_jsonl(&output_jsonl, &telemetry)?;
        return Ok(final_answer);
    }

    let final_answer = match runner_output.final_answer {
        Some(value) => value,
        None => {
            let text = runner_output.final_output.ok_or_else(|| {
                HarnessError::Http("Agent SDK runner did not return final output".to_owned())
            })?;
            serde_json::from_str::<Value>(&text)
                .map_err(|err| HarnessError::Json(format!("final answer is not JSON: {err}")))?
        }
    };
    let conn = Connection::open_with_flags(
        sqlite_path(&data_root),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(from_sql)?;
    let missing_evidence_count = final_answer
        .get("missing_evidence")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let judge_result = match judge_answer(&conn, &task, arm, &final_answer, None) {
        Ok(judge_result) => judge_result,
        Err(err) if is_recoverable_run_judge_error(&err) => {
            let error = err.to_string();
            let mut derived = DerivedMetrics {
                interface_card_tokens: estimate_tokens(arm_card),
                task_prompt_tokens: estimate_tokens(&task.prompt),
                wall_time_ms: completed.saturating_sub(started),
                task_success: Some(false),
                evidence_precision: None,
                tool_call_count: bridge_snapshot.tool_call_count(),
                tool_result_tokens: bridge_snapshot.tool_result_tokens,
                tool_bytes_read: bridge_snapshot.tool_bytes_read,
                invalid_sql_count: bridge_snapshot.invalid_sql_count,
                wrong_tool_count: bridge_snapshot.wrong_tool_count,
                tool_timeout_count: bridge_snapshot.tool_timeout_count,
                missing_evidence_count,
                overread_bytes: 0,
                ..DerivedMetrics::default()
            };
            apply_usage_costs(&mut derived, &api_calls, &model);
            let telemetry = BenchmarkRunTelemetry {
                record_type: "benchmark_run".to_owned(),
                run_id,
                arm_id: arm.to_owned(),
                task_id: task.task_id,
                repeat_index,
                model,
                started_at_unix_ms: started,
                completed_at_unix_ms: completed,
                api_calls,
                tool_calls: bridge_snapshot.tool_calls,
                derived_metrics: derived,
                correctness: false,
                judge: None,
                final_answer: Some(final_answer.clone()),
                run_error: Some(error),
            };
            append_jsonl(&output_jsonl, &telemetry)?;
            return Ok(final_answer);
        }
        Err(err) => return Err(err),
    };
    let mut derived = DerivedMetrics {
        interface_card_tokens: estimate_tokens(arm_card),
        task_prompt_tokens: estimate_tokens(&task.prompt),
        wall_time_ms: completed.saturating_sub(started),
        task_success: Some(judge_result.task_success),
        evidence_precision: judge_result.evidence_precision,
        tool_call_count: bridge_snapshot.tool_call_count(),
        tool_result_tokens: bridge_snapshot.tool_result_tokens,
        tool_bytes_read: bridge_snapshot.tool_bytes_read,
        invalid_sql_count: bridge_snapshot.invalid_sql_count,
        wrong_tool_count: bridge_snapshot.wrong_tool_count,
        tool_timeout_count: bridge_snapshot.tool_timeout_count,
        missing_evidence_count,
        overread_bytes: 0,
        ..DerivedMetrics::default()
    };
    apply_usage_costs(&mut derived, &api_calls, &model);
    let telemetry = BenchmarkRunTelemetry {
        record_type: "benchmark_run".to_owned(),
        run_id,
        arm_id: arm.to_owned(),
        task_id: task.task_id,
        repeat_index,
        model,
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        api_calls,
        tool_calls: bridge_snapshot.tool_calls,
        derived_metrics: derived,
        correctness: judge_result.task_success,
        judge: Some(judge_result),
        final_answer: Some(final_answer.clone()),
        run_error: None,
    };
    append_jsonl(&output_jsonl, &telemetry)?;
    Ok(final_answer)
}

fn run_python_agent_runner(config: &AgentRunnerConfig) -> Result<AgentRunnerOutput, HarnessError> {
    let config_file = tempfile::NamedTempFile::new().map_err(from_io)?;
    serde_json::to_writer(config_file.as_file(), config)
        .map_err(|err| HarnessError::Json(err.to_string()))?;
    let script = agent_runner_script_path();
    let python = env::var("PYTHON").unwrap_or_else(|_| "python3".to_owned());
    let harness_bin = env::var("YANEX_BENCH_HARNESS_BIN")
        .map(PathBuf::from)
        .or_else(|_| env::current_exe().map_err(|err| err.to_string()))
        .map_err(HarnessError::Io)?;
    let output = Command::new(python)
        .arg(script)
        .arg("run")
        .arg("--config")
        .arg(config_file.path())
        .arg("--harness-bin")
        .arg(harness_bin)
        .arg("--arm")
        .arg(&config.arm)
        .output()
        .map_err(from_io)?;
    if !output.status.success() {
        return Err(HarnessError::Http(format!(
            "Agent SDK runner failed with status {}: stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    serde_json::from_slice(&output.stdout).map_err(|err| {
        HarnessError::Json(format!(
            "Agent SDK runner output parse error: {err}; stdout={}",
            String::from_utf8_lossy(&output.stdout)
        ))
    })
}

fn agent_runner_script_path() -> PathBuf {
    env::var("YANEX_BENCH_AGENT_SDK_RUNNER")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("agent-interface/agents_runner/openai_agents_responses_schema_once.py")
        })
}

fn append_tool_call_start_jsonl(
    path: &Path,
    run_id: &str,
    arm: &str,
    task_id: &str,
    repeat_index: usize,
    call: &OpenAiToolCall,
    started_at_unix_ms: u128,
) -> Result<(), HarnessError> {
    let telemetry = ToolCallStartTelemetry {
        record_type: "tool_call_start".to_owned(),
        run_id: run_id.to_owned(),
        arm_id: arm.to_owned(),
        task_id: task_id.to_owned(),
        repeat_index,
        call_id: call.id.clone(),
        tool_name: call.function.name.clone(),
        arguments: tool_call_arguments(call),
        started_at_unix_ms,
    };
    append_jsonl(path, &telemetry)
}

fn append_jsonl(path: &Path, record: &impl Serialize) -> Result<(), HarnessError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(from_io)?;
    let line = serde_json::to_string(record).map_err(|err| HarnessError::Json(err.to_string()))?;
    file.write_all(line.as_bytes()).map_err(from_io)?;
    file.write_all(b"\n").map_err(from_io)
}

fn tool_call_arguments(call: &OpenAiToolCall) -> Value {
    serde_json::from_str(&call.function.arguments)
        .unwrap_or_else(|_| json!({"raw": call.function.arguments.clone()}))
}

fn run_error_answer(
    task: &BenchmarkTask,
    error: &str,
    tool_call_count: usize,
    bytes_read_estimate: usize,
) -> Value {
    json!({
        "task_id": task.task_id.clone(),
        "status": "failed",
        "answer": Value::Null,
        "run_ids": [],
        "evidence": [],
        "missing_evidence": [error],
        "operations_summary": {
            "tool_call_count": tool_call_count,
            "bytes_read_estimate": bytes_read_estimate,
            "notes": "run terminated after a fatal tool execution error"
        },
        "confidence": "low"
    })
}

fn estimate_tokens(text: &str) -> usize {
    text.split_whitespace().count().max(text.len() / 4)
}

fn benchmark_messages(
    profile: &LoadedBaseProfile,
    arm_card: &str,
    task: &BenchmarkTask,
) -> Vec<ChatMessage> {
    vec![
        ChatMessage {
            role: "system".to_owned(),
            content: Some(profile.base_system_message.trim_end().to_owned()),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: "system".to_owned(),
            content: Some(format!(
                "{}\n\nCurrent arm card YAML:\n{}",
                profile.base_developer_message.trim_end(),
                arm_card
            )),
            tool_call_id: None,
            tool_calls: None,
        },
        ChatMessage {
            role: "user".to_owned(),
            content: Some(format!(
                "Task ID: {}\nCategory: {}\n\n{}",
                task.task_id, task.category, task.prompt
            )),
            tool_call_id: None,
            tool_calls: None,
        },
    ]
}

fn arm_card_yaml(arm: &str) -> Result<&'static str, HarnessError> {
    match arm {
        "sqlite_raw_v1" => Ok(include_str!("../../agent-interface/arms/sqlite_raw.yaml")),
        "nokv_native_v1" => Ok(include_str!("../../agent-interface/arms/nokv_native.yaml")),
        other => Err(HarnessError::Corpus(format!(
            "unknown benchmark arm {other}"
        ))),
    }
}

fn response_input_messages(messages: &[ChatMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|message| {
            json!({
                "role": message.role,
                "content": message.content.as_deref().unwrap_or_default()
            })
        })
        .collect()
}

fn response_function_call_output(call_id: &str, content: &Value) -> Value {
    json!({
        "type": "function_call_output",
        "call_id": call_id,
        "output": content.to_string()
    })
}

fn response_text_config(response_format: Option<Value>) -> Option<ResponseTextConfig> {
    response_format.map(|format| ResponseTextConfig { format })
}

fn response_tools(tools: &[ToolDefinition]) -> Vec<ResponseTool> {
    tools
        .iter()
        .map(|tool| ResponseTool {
            tool_type: "function".to_owned(),
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: tool.parameters.clone(),
        })
        .collect()
}

fn response_tool_calls(
    response: &ResponseApiResponse,
) -> Result<Vec<OpenAiToolCall>, HarnessError> {
    let mut calls = Vec::new();
    for item in &response.output {
        if item.get("type").and_then(Value::as_str) != Some("function_call") {
            continue;
        }
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .ok_or_else(|| HarnessError::Http("OpenAI function_call missing call_id".to_owned()))?;
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| HarnessError::Http("OpenAI function_call missing name".to_owned()))?;
        let arguments = item
            .get("arguments")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                HarnessError::Http("OpenAI function_call missing arguments".to_owned())
            })?;
        calls.push(OpenAiToolCall {
            id: call_id.to_owned(),
            tool_type: "function".to_owned(),
            function: OpenAiToolCallFunction {
                name: name.to_owned(),
                arguments: arguments.to_owned(),
            },
        });
    }
    Ok(calls)
}

fn response_final_text(response: &ResponseApiResponse) -> Option<String> {
    if let Some(output_text) = response.output_text.as_ref() {
        if !output_text.is_empty() {
            return Some(output_text.clone());
        }
    }

    let mut text = String::new();
    for item in &response.output {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        let Some(content) = item.get("content") else {
            continue;
        };
        if let Some(raw) = content.as_str() {
            text.push_str(raw);
            continue;
        }
        let Some(parts) = content.as_array() else {
            continue;
        };
        for part in parts {
            if part.get("type").and_then(Value::as_str) == Some("output_text") {
                if let Some(part_text) = part.get("text").and_then(Value::as_str) {
                    text.push_str(part_text);
                }
            }
        }
    }
    (!text.is_empty()).then_some(text)
}

fn send_model_response(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    api_key: &str,
    request: &ResponseRequest,
) -> Result<ResponseApiResponse, HarnessError> {
    for attempt in 1..=RESPONSE_MAX_ATTEMPTS {
        let response = match client
            .post(endpoint)
            .bearer_auth(api_key)
            .json(request)
            .send()
        {
            Ok(response) => response,
            Err(err) if attempt < RESPONSE_MAX_ATTEMPTS => {
                log_model_response_send_error(attempt, &err);
                thread::sleep(Duration::from_millis(500 * attempt as u64));
                if err.is_timeout() || err.is_connect() || err.is_request() {
                    continue;
                }
                return Err(HarnessError::Http(err.to_string()));
            }
            Err(err) => {
                log_model_response_send_error(attempt, &err);
                return Err(HarnessError::Http(err.to_string()));
            }
        };
        let status = response.status();
        let text = response
            .text()
            .map_err(|err| HarnessError::Http(err.to_string()))?;
        if !status.is_success() {
            if attempt < RESPONSE_MAX_ATTEMPTS
                && (status.as_u16() == 429 || status.is_server_error())
            {
                thread::sleep(Duration::from_millis(500 * attempt as u64));
                continue;
            }
            return Err(HarnessError::Http(format!(
                "OpenAI response HTTP {status}: {text}"
            )));
        }
        return serde_json::from_str(&text).map_err(|err| {
            HarnessError::Json(format!("OpenAI response parse error: {err}; {text}"))
        });
    }
    unreachable!("response attempts loop always returns")
}

fn log_model_response_send_error(attempt: usize, err: &reqwest::Error) {
    eprintln!(
        "{}",
        model_response_send_error_diagnostic(attempt, RESPONSE_MAX_ATTEMPTS, err)
    );
}

fn model_response_send_error_diagnostic(
    attempt: usize,
    max_attempts: usize,
    err: &reqwest::Error,
) -> String {
    let mut diagnostic = format!(
        "OpenAI response send error: attempt={attempt}/{max_attempts} \
         is_timeout={} is_connect={} is_request={} error={err}",
        err.is_timeout(),
        err.is_connect(),
        err.is_request()
    );
    let mut source = err.source();
    let mut index = 0;
    while let Some(err) = source {
        diagnostic.push_str(&format!("\n  source[{index}]={err}"));
        source = err.source();
        index += 1;
    }
    if index == 0 {
        diagnostic.push_str("\n  source=<none>");
    }
    diagnostic
}

fn is_recoverable_run_api_error(err: &HarnessError) -> bool {
    let HarnessError::Http(message) = err else {
        return false;
    };
    message.contains("context_length_exceeded")
        || message.contains("Input tokens exceed the configured limit")
        || message.contains("model did not produce a final JSON answer before max_turns")
}

fn is_recoverable_run_judge_error(err: &HarnessError) -> bool {
    matches!(err, HarnessError::Judge(_))
}

fn api_call_telemetry(
    response: &ResponseApiResponse,
    requested_model: &str,
    started: u128,
    completed: u128,
    previous_response_id: Option<String>,
    sent_tool_schema: bool,
    sent_initial_instructions: bool,
) -> ApiCallTelemetry {
    let usage = response.usage.as_ref();
    ApiCallTelemetry {
        request_id: None,
        response_id: response.id.clone(),
        model: response
            .model
            .clone()
            .unwrap_or_else(|| requested_model.to_owned()),
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        previous_response_id,
        sent_tool_schema,
        sent_initial_instructions,
        prompt_tokens: usage.and_then(|usage| usage.input_tokens),
        completion_tokens: usage.and_then(|usage| usage.output_tokens),
        total_tokens: usage.and_then(|usage| usage.total_tokens),
        reasoning_tokens: usage
            .and_then(|usage| usage.output_tokens_details.as_ref())
            .and_then(|details| details.reasoning_tokens),
        cached_prompt_tokens: usage
            .and_then(|usage| usage.input_tokens_details.as_ref())
            .and_then(|details| details.cached_tokens),
        accepted_prediction_tokens: usage
            .and_then(|usage| usage.output_tokens_details.as_ref())
            .and_then(|details| details.accepted_prediction_tokens),
        rejected_prediction_tokens: usage
            .and_then(|usage| usage.output_tokens_details.as_ref())
            .and_then(|details| details.rejected_prediction_tokens),
    }
}

fn apply_usage_costs(metrics: &mut DerivedMetrics, api_calls: &[ApiCallTelemetry], _model: &str) {
    metrics.tool_schema_repeated_count = tool_schema_repeated_count(api_calls);
    let input_rate = env::var("OPENAI_INPUT_USD_PER_1M_TOKENS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    let output_rate = env::var("OPENAI_OUTPUT_USD_PER_1M_TOKENS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok());
    let (Some(input_rate), Some(output_rate)) = (input_rate, output_rate) else {
        return;
    };
    let cached_rate = env::var("OPENAI_CACHED_INPUT_USD_PER_1M_TOKENS")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(input_rate);
    let prompt_tokens = api_calls
        .iter()
        .filter_map(|call| call.prompt_tokens)
        .sum::<u64>() as f64;
    let cached_tokens = api_calls
        .iter()
        .filter_map(|call| call.cached_prompt_tokens)
        .sum::<u64>() as f64;
    let completion_tokens = api_calls
        .iter()
        .filter_map(|call| call.completion_tokens)
        .sum::<u64>() as f64;
    let uncached_tokens = (prompt_tokens - cached_tokens).max(0.0);
    let cost = uncached_tokens * input_rate / 1_000_000.0
        + cached_tokens * cached_rate / 1_000_000.0
        + completion_tokens * output_rate / 1_000_000.0;
    metrics.execution_cost_usd = Some(cost);
    metrics.all_in_cost_usd = Some(cost);
}

fn tool_schema_repeated_count(api_calls: &[ApiCallTelemetry]) -> usize {
    api_calls
        .iter()
        .filter(|call| call.previous_response_id.is_some() && call.sent_tool_schema)
        .count()
}

impl ToolWorker {
    fn start(
        arm: &str,
        data_root: &Path,
        s3: &S3Options,
        registry: &[ToolDefinition],
        timeout: Duration,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel::<ToolWorkerRequest>();
        let arm = arm.to_owned();
        let data_root = data_root.to_owned();
        let s3 = s3.clone();
        let registry = registry.to_vec();
        thread::spawn(move || {
            let runtime = ArmRuntime::open(&arm, &data_root, &s3);
            match runtime {
                Ok(mut runtime) => {
                    for request in request_rx {
                        let outcome = runtime.execute_tool_call(&arm, &registry, &request.call);
                        let _ = request.response_tx.send(outcome);
                    }
                }
                Err(err) => {
                    let error = err.to_string();
                    for request in request_rx {
                        let _ = request.response_tx.send(tool_error_outcome(
                            error.clone(),
                            false,
                            false,
                        ));
                    }
                }
            }
        });
        Self {
            request_tx,
            timeout,
        }
    }

    fn execute_tool_call(&self, call: &OpenAiToolCall) -> ToolExecutionOutcome {
        let (response_tx, response_rx) = mpsc::channel();
        if self
            .request_tx
            .send(ToolWorkerRequest {
                call: call.clone(),
                response_tx,
            })
            .is_err()
        {
            return tool_error_outcome("tool worker is not available".to_owned(), false, false);
        }
        match response_rx.recv_timeout(self.timeout) {
            Ok(outcome) => outcome,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                tool_timeout_outcome(call.function.name.clone(), self.timeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => tool_error_outcome(
                "tool worker stopped before returning a result".to_owned(),
                false,
                false,
            ),
        }
    }
}

impl ToolBridgeServer {
    fn start(config: ToolBridgeStartConfig<'_>) -> Result<Self, HarnessError> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(from_io)?;
        listener.set_nonblocking(true).map_err(from_io)?;
        let url = format!("http://{}", listener.local_addr().map_err(from_io)?);
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let run_id = config.run_id.to_owned();
        let arm = config.arm.to_owned();
        let task_id = config.task_id.to_owned();
        let repeat_index = config.repeat_index;
        let data_root = config.data_root.to_owned();
        let s3 = config.s3.clone();
        let registry = config.registry.to_vec();
        let timeout = config.timeout;
        let output_jsonl = config.output_jsonl;
        let handle = thread::spawn(move || {
            let tool_worker = ToolWorker::start(&arm, &data_root, &s3, &registry, timeout);
            let mut snapshot = ToolBridgeSnapshot::default();
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // BSD-style platforms let accepted sockets inherit the
                        // listener's non-blocking flag; an early read would then
                        // fail with WouldBlock before the request body arrives.
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
                        let context = ToolBridgeConnectionContext {
                            run_id: &run_id,
                            arm: &arm,
                            task_id: &task_id,
                            repeat_index,
                            output_jsonl: &output_jsonl,
                            tool_worker: &tool_worker,
                        };
                        if let Err(err) =
                            handle_tool_bridge_connection(&mut stream, &context, &mut snapshot)
                        {
                            let outcome = tool_error_outcome(
                                format!("tool bridge error: {err}"),
                                false,
                                false,
                            );
                            let response = ToolBridgeInvokeResponse {
                                status: outcome.status.clone(),
                                content: outcome.content.clone(),
                                bytes_read: outcome.bytes_read,
                                result_token_estimate: outcome.result_token_estimate,
                                error: outcome.error.clone(),
                                timed_out: outcome.timed_out,
                                fatal_run_error: outcome.fatal_run_error,
                            };
                            let _ = write_http_json_response(&mut stream, "200 OK", &response);
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(err) => return Err(from_io(err)),
                }
            }
            Ok(snapshot)
        });
        Ok(Self {
            url,
            shutdown_tx,
            handle: Some(handle),
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn finish(mut self) -> Result<ToolBridgeSnapshot, HarnessError> {
        let _ = self.shutdown_tx.send(());
        let _ = reqwest::blocking::Client::new()
            .post(format!("{}/shutdown", self.url))
            .send();
        let handle = self
            .handle
            .take()
            .ok_or_else(|| HarnessError::Http("tool bridge already stopped".to_owned()))?;
        handle
            .join()
            .map_err(|_| HarnessError::Http("tool bridge thread panicked".to_owned()))?
    }
}

impl Drop for ToolBridgeServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl ToolBridgeSnapshot {
    fn tool_call_count(&self) -> usize {
        self.tool_calls.len()
    }

    fn record(&mut self, telemetry: ToolCallTelemetry, outcome: &ToolExecutionOutcome) {
        self.invalid_sql_count += usize::from(outcome.invalid_sql);
        self.wrong_tool_count += usize::from(outcome.wrong_tool);
        self.tool_timeout_count += usize::from(outcome.timed_out);
        self.tool_bytes_read += outcome.bytes_read;
        self.tool_result_tokens += outcome.result_token_estimate;
        if outcome.fatal_run_error {
            self.fatal_error = Some(outcome.error.clone().unwrap_or_else(|| {
                format!("tool {} failed with a fatal run error", telemetry.tool_name)
            }));
        }
        self.tool_calls.push(telemetry);
    }
}

fn handle_tool_bridge_connection(
    stream: &mut std::net::TcpStream,
    context: &ToolBridgeConnectionContext<'_>,
    snapshot: &mut ToolBridgeSnapshot,
) -> Result<(), HarnessError> {
    let request = read_http_request(stream)?;
    if request.path == "/shutdown" {
        write_http_json_response(stream, "200 OK", &json!({"status": "ok"}))?;
        return Ok(());
    }
    if request.path != "/invoke" {
        write_http_json_response(
            stream,
            "404 Not Found",
            &json!({"status": "error", "error": "unknown tool bridge path"}),
        )?;
        return Ok(());
    }
    let payload: ToolBridgeInvokeRequest =
        serde_json::from_slice(&request.body).map_err(|err| HarnessError::Json(err.to_string()))?;
    if payload.run_id != context.run_id {
        write_http_json_response(
            stream,
            "400 Bad Request",
            &json!({
                "status": "error",
                "error": format!("tool bridge run_id mismatch: expected {}, got {}", context.run_id, payload.run_id)
            }),
        )?;
        return Ok(());
    }
    let call_id = format!("bridge-call-{}", snapshot.tool_calls.len());
    let arguments = serde_json::to_string(&payload.arguments)
        .map_err(|err| HarnessError::Json(err.to_string()))?;
    let call = OpenAiToolCall {
        id: call_id.clone(),
        tool_type: "function".to_owned(),
        function: OpenAiToolCallFunction {
            name: payload.tool_name,
            arguments,
        },
    };
    let started = now_unix_ms();
    append_tool_call_start_jsonl(
        context.output_jsonl,
        context.run_id,
        context.arm,
        context.task_id,
        context.repeat_index,
        &call,
        started,
    )?;
    let outcome = context.tool_worker.execute_tool_call(&call);
    let completed = now_unix_ms();
    let telemetry = ToolCallTelemetry {
        call_id,
        tool_name: call.function.name.clone(),
        arguments: tool_call_arguments(&call),
        started_at_unix_ms: started,
        completed_at_unix_ms: completed,
        status: outcome.status.clone(),
        bytes_read: outcome.bytes_read,
        result_token_estimate: outcome.result_token_estimate,
        error: outcome.error.clone(),
        timed_out: outcome.timed_out,
    };
    let response = ToolBridgeInvokeResponse {
        status: outcome.status.clone(),
        content: outcome.content.clone(),
        bytes_read: outcome.bytes_read,
        result_token_estimate: outcome.result_token_estimate,
        error: outcome.error.clone(),
        timed_out: outcome.timed_out,
        fatal_run_error: outcome.fatal_run_error,
    };
    snapshot.record(telemetry, &outcome);
    write_http_json_response(stream, "200 OK", &response)
}

fn read_http_request(stream: &mut std::net::TcpStream) -> Result<HttpRequest, HarnessError> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buffer).map_err(from_io)?;
        if read == 0 {
            return Err(HarnessError::Http(
                "HTTP request closed before headers completed".to_owned(),
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if let Some((header_end, content_length, path)) = http_body_bounds(&bytes)? {
            let body_start = header_end + 4;
            while bytes.len() < body_start + content_length {
                let read = stream.read(&mut buffer).map_err(from_io)?;
                if read == 0 {
                    return Err(HarnessError::Http(
                        "HTTP request closed before body completed".to_owned(),
                    ));
                }
                bytes.extend_from_slice(&buffer[..read]);
            }
            return Ok(HttpRequest {
                path,
                body: bytes[body_start..body_start + content_length].to_vec(),
            });
        }
    }
}

fn http_body_bounds(bytes: &[u8]) -> Result<Option<(usize, usize, String)>, HarnessError> {
    let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(None);
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .map_err(|err| HarnessError::Http(err.to_string()))?;
    let request_line = headers
        .lines()
        .next()
        .ok_or_else(|| HarnessError::Http("HTTP request missing request line".to_owned()))?;
    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| HarnessError::Http("HTTP request missing path".to_owned()))?
        .to_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .map_err(|err| HarnessError::Http(err.to_string()))?
        .unwrap_or(0);
    Ok(Some((header_end, content_length, path)))
}

fn write_http_json_response(
    stream: &mut std::net::TcpStream,
    status: &str,
    value: &impl Serialize,
) -> Result<(), HarnessError> {
    let body = serde_json::to_string(value).map_err(|err| HarnessError::Json(err.to_string()))?;
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).map_err(from_io)
}

impl ArmRuntime {
    fn open(arm: &str, data_root: &Path, s3: &S3Options) -> Result<Self, HarnessError> {
        match arm {
            "sqlite_raw_v1" => {
                ensure_sqlite_agent_index_materialization(data_root)?;
                Ok(Self::SqliteRaw {
                    conn: Connection::open_with_flags(
                        sqlite_path(data_root),
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                    )
                    .map_err(from_sql)?,
                })
            }
            "nokv_native_v1" => Ok(Self::NokvNative {
                service: Box::new(open_existing_nokv(&nokv_meta_path(data_root), s3)?),
            }),
            other => Err(HarnessError::Corpus(format!(
                "unknown benchmark arm {other}"
            ))),
        }
    }

    fn execute_tool_call(
        &mut self,
        arm: &str,
        registry: &[ToolDefinition],
        call: &OpenAiToolCall,
    ) -> ToolExecutionOutcome {
        if !registry.iter().any(|tool| tool.name == call.function.name) {
            return tool_error_outcome(
                format!(
                    "tool {} is not registered for arm {arm}",
                    call.function.name
                ),
                false,
                true,
            );
        }
        let args = match serde_json::from_str::<Value>(&call.function.arguments) {
            Ok(args) => args,
            Err(err) => {
                return tool_error_outcome(
                    format!("tool arguments are not valid JSON: {err}"),
                    false,
                    false,
                )
            }
        };
        let result = match self {
            Self::SqliteRaw { conn } => execute_sqlite_raw_tool(conn, &call.function.name, &args),
            Self::NokvNative { service } => {
                execute_nokv_tool(service.as_ref(), &call.function.name, &args)
            }
        };
        match result {
            Ok(content) => tool_success_outcome(content),
            Err(err) => {
                if matches!(err, HarnessError::ToolTimeout { .. }) {
                    return tool_timeout_error_outcome(err.to_string());
                }
                let invalid_sql =
                    matches!(self, Self::SqliteRaw { .. }) && call.function.name == "query_sql";
                tool_error_outcome(err.to_string(), invalid_sql, false)
            }
        }
    }
}

fn execute_sqlite_raw_tool(
    conn: &Connection,
    name: &str,
    args: &Value,
) -> Result<Value, HarnessError> {
    match name {
        "show_schema" => Ok(json!({
            "tool": "show_schema",
            "schema": sqlite_schema_text(conn)?,
            "evidence": ["sqlite://schema"]
        })),
        "query_sql" => {
            let sql = required_string_arg(args, "sql")?;
            to_json_value(execute_sqlite_query_tool(conn, &sql)?)
        }
        "read_blob" => {
            let blob_ref = required_string_arg(args, "blob_ref")?;
            let offset = optional_u64_arg(args, "offset").unwrap_or(0);
            let limit = required_usize_arg(args, "limit")?;
            to_json_value(read_blob_tool(conn, &blob_ref, offset, limit)?)
        }
        "grep_blob" => {
            let blob_ref = required_string_arg(args, "blob_ref")?;
            let pattern = required_string_arg(args, "pattern")?;
            let cursor = optional_string_arg(args, "cursor");
            let limit = optional_usize_arg(args, "limit")?
                .unwrap_or(100)
                .clamp(1, 100);
            grep_blob_tool(conn, &blob_ref, &pattern, cursor.as_deref(), limit)
        }
        other => Err(HarnessError::Corpus(format!(
            "unknown sqlite raw tool {other}"
        ))),
    }
}

fn execute_nokv_tool<M, O>(
    service: &NoKvFs<M, O>,
    name: &str,
    args: &Value,
) -> Result<Value, HarnessError>
where
    M: MetadataStore,
    O: ObjectStore,
{
    match name {
        "ls" | "stat" | "catalog" | "read" | "find" | "aggregate" | "grep" => {
            execute_agent_tool(service, name, args).map_err(from_nokv)
        }
        other => Err(HarnessError::Corpus(format!(
            "unknown NoKV native tool {other}"
        ))),
    }
}

fn to_json_value(value: impl Serialize) -> Result<Value, HarnessError> {
    serde_json::to_value(value).map_err(|err| HarnessError::Json(err.to_string()))
}

fn sqlite_schema_text(conn: &Connection) -> Result<String, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT tbl_name, sql FROM sqlite_schema WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(from_sql)?;
    let mut out = String::new();
    for row in rows {
        let (table_name, sql) = row.map_err(from_sql)?;
        if !sqlite_schema_visible_table(&table_name) {
            continue;
        }
        out.push_str(&sql);
        out.push('\n');
    }
    Ok(out)
}

fn sqlite_schema_visible_table(table_name: &str) -> bool {
    matches!(
        table_name,
        "artifacts"
            | "blobs"
            | "dependencies"
            | "experiments"
            | "files"
            | "git_state"
            | "metrics"
            | "params"
            | "run_agent_index_catalog"
            | "run_agent_index_values"
    )
}

fn grep_blob_tool(
    conn: &Connection,
    blob_ref: &str,
    pattern: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value, HarnessError> {
    let bytes = query_single_blob(
        conn,
        "SELECT content FROM blobs WHERE blob_ref = ?1",
        blob_ref,
    )?;
    if bytes.contains(&0) {
        return Err(HarnessError::Corpus(format!(
            "blob {blob_ref} is not text; use read_blob for binary content"
        )));
    }
    let start_line = match cursor {
        Some(cursor) => cursor
            .parse::<usize>()
            .map_err(|_| HarnessError::Corpus(format!("invalid grep_blob cursor {cursor}")))?,
        None => 0,
    };
    let pattern_lower = pattern.to_lowercase();
    let text = String::from_utf8_lossy(&bytes);
    let mut matches = Vec::new();
    let mut next_cursor = None;
    for (line_index, line) in text.lines().enumerate().skip(start_line) {
        if !line.to_lowercase().contains(&pattern_lower) {
            continue;
        }
        if matches.len() == limit {
            next_cursor = Some(line_index.to_string());
            break;
        }
        matches.push(json!({
            "line_number": line_index + 1,
            "snippet": line.chars().take(240).collect::<String>(),
            "evidence": format!("sqlite://blobs/{blob_ref}#L{}", line_index + 1),
        }));
    }
    Ok(json!({
        "tool": "grep_blob",
        "blob_ref": blob_ref,
        "pattern": pattern,
        "total_size_bytes": bytes.len(),
        "bytes_read": bytes.len(),
        "matches": matches,
        "truncated": next_cursor.is_some(),
        "next_cursor": next_cursor,
    }))
}

fn required_string_arg(args: &Value, name: &'static str) -> Result<String, HarnessError> {
    args.get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(HarnessError::MissingOption(name))
}

fn optional_string_arg(args: &Value, name: &'static str) -> Option<String> {
    args.get(name).and_then(Value::as_str).map(str::to_owned)
}

fn required_usize_arg(args: &Value, name: &'static str) -> Result<usize, HarnessError> {
    args.get(name)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(HarnessError::MissingOption(name))
}

fn optional_usize_arg(args: &Value, name: &'static str) -> Result<Option<usize>, HarnessError> {
    let Some(value) = args.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .map(Some)
        .ok_or(HarnessError::InvalidNumber {
            option: name,
            value: value.to_string(),
        })
}

fn optional_u64_arg(args: &Value, name: &'static str) -> Option<u64> {
    args.get(name).and_then(Value::as_u64)
}

fn tool_success_outcome(content: Value) -> ToolExecutionOutcome {
    let result_text = content.to_string();
    ToolExecutionOutcome {
        bytes_read: bytes_read_from_tool_result(&content),
        result_token_estimate: estimate_tokens(&result_text),
        content,
        status: "success".to_owned(),
        error: None,
        invalid_sql: false,
        wrong_tool: false,
        timed_out: false,
        fatal_run_error: false,
    }
}

fn tool_error_outcome(error: String, invalid_sql: bool, wrong_tool: bool) -> ToolExecutionOutcome {
    let content = json!({
        "status": "error",
        "error": error.clone(),
        "invalid_sql": invalid_sql,
        "wrong_tool": wrong_tool
    });
    let result_text = content.to_string();
    ToolExecutionOutcome {
        content,
        bytes_read: 0,
        result_token_estimate: estimate_tokens(&result_text),
        status: "error".to_owned(),
        error: Some(error),
        invalid_sql,
        wrong_tool,
        timed_out: false,
        fatal_run_error: false,
    }
}

fn tool_timeout_outcome(tool_name: String, timeout: Duration) -> ToolExecutionOutcome {
    tool_timeout_error_outcome(format!(
        "tool {tool_name} timed out after {}ms",
        duration_millis(timeout)
    ))
}

fn tool_timeout_error_outcome(error: String) -> ToolExecutionOutcome {
    let content = json!({
        "status": "error",
        "error": error.clone(),
        "timed_out": true
    });
    let result_text = content.to_string();
    ToolExecutionOutcome {
        content,
        bytes_read: 0,
        result_token_estimate: estimate_tokens(&result_text),
        status: "timeout".to_owned(),
        error: Some(error),
        invalid_sql: false,
        wrong_tool: false,
        timed_out: true,
        fatal_run_error: true,
    }
}

fn bytes_read_from_tool_result(value: &Value) -> usize {
    if let Some(bytes) = value.get("bytes_read").and_then(Value::as_u64) {
        return usize::try_from(bytes).unwrap_or(usize::MAX);
    }
    value
        .get("bytes_read")
        .and_then(Value::as_i64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_else(|| {
            value
                .get("bytes_read_estimate")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(0)
        })
}

fn judge_answer(
    conn: &Connection,
    task: &BenchmarkTask,
    arm: &str,
    answer: &Value,
    nokv_native: Option<&NoKvFs<HoltMetadataStore, S3ObjectStore>>,
) -> Result<JudgeResult, HarnessError> {
    let gold_rows = query_gold_rows(conn, task)?;
    let mut mismatches = Vec::new();
    let expected_run_ids = expected_run_ids(&gold_rows, task.expected.run_id_column.as_deref());
    let actual_run_ids = actual_run_ids(answer, &task.expected);
    if !expected_run_ids.is_empty() && actual_run_ids != expected_run_ids {
        mismatches.push(format!(
            "run_ids differ; expected {:?}, got {:?}",
            expected_run_ids, actual_run_ids
        ));
    }

    let answer_root = answer;
    match task.expected.kind.as_str() {
        "status_counts" => {
            let key = task.expected.answer_key.as_deref().ok_or_else(|| {
                HarnessError::Judge("status_counts requires answer_key".to_owned())
            })?;
            let actual_rows = answer_root
                .get(key)
                .and_then(Value::as_array)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} must be an array")))?;
            compare_row_arrays(
                &gold_rows,
                actual_rows,
                &task.expected.columns,
                &mut mismatches,
            );
            let expected_total = gold_rows
                .iter()
                .filter_map(|row| row.get("count").and_then(sqlite_value_as_i64))
                .sum::<i64>();
            let actual_total = answer_root
                .get("total_runs")
                .and_then(sqlite_value_as_i64)
                .unwrap_or(-1);
            if expected_total != actual_total {
                mismatches.push(format!(
                    "answer.total_runs differs; expected {expected_total}, got {actual_total}"
                ));
            }
        }
        "rows_exact" => {
            let key =
                task.expected.answer_key.as_deref().ok_or_else(|| {
                    HarnessError::Judge("rows_exact requires answer_key".to_owned())
                })?;
            let actual_rows = answer_root
                .get(key)
                .and_then(Value::as_array)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} must be an array")))?;
            compare_row_arrays(
                &gold_rows,
                actual_rows,
                &task.expected.columns,
                &mut mismatches,
            );
        }
        "first_row_object" => {
            let key = task.expected.answer_key.as_deref().ok_or_else(|| {
                HarnessError::Judge("first_row_object requires answer_key".to_owned())
            })?;
            let actual = answer_root
                .get(key)
                .ok_or_else(|| HarnessError::Judge(format!("answer.{key} is missing")))?;
            let expected = gold_rows.first().cloned().unwrap_or(Value::Null);
            compare_objects(
                &expected,
                actual,
                &task.expected.columns,
                "answer",
                &mut mismatches,
            );
        }
        "index_count_consistency" => {
            judge_index_count_consistency(&gold_rows, answer_root, &mut mismatches)?;
        }
        other => mismatches.push(format!("unsupported expected kind {other}")),
    }

    let (evidence_checked, evidence_supported) = evidence_support(conn, arm, answer, nokv_native)?;
    let evidence_precision =
        (evidence_checked > 0).then_some(evidence_supported as f64 / evidence_checked as f64);
    Ok(JudgeResult {
        task_success: mismatches.is_empty(),
        evidence_precision,
        evidence_checked,
        evidence_supported,
        expected_run_ids,
        actual_run_ids,
        mismatches,
    })
}

fn actual_run_ids(answer: &Value, expected: &ExpectedSpec) -> Vec<String> {
    let Some(column) = expected.run_id_column.as_deref() else {
        return Vec::new();
    };
    match expected.kind.as_str() {
        "rows_exact" => expected
            .answer_key
            .as_deref()
            .and_then(|key| answer.get(key))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|row| row.get(column).and_then(Value::as_str))
            .map(str::to_owned)
            .collect(),
        "first_row_object" => expected
            .answer_key
            .as_deref()
            .and_then(|key| answer.get(key))
            .and_then(|object| object.get(column))
            .and_then(Value::as_str)
            .map(|run_id| vec![run_id.to_owned()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn query_gold_rows(conn: &Connection, task: &BenchmarkTask) -> Result<Vec<Value>, HarnessError> {
    if let Some(sql) = task
        .gold_sql
        .as_deref()
        .filter(|sql| !sql.trim().is_empty())
    {
        return execute_sqlite_query_tool(conn, sql).map(|result| {
            result
                .rows
                .into_iter()
                .map(|row| row.row)
                .collect::<Vec<_>>()
        });
    }
    query_oracle_rows(conn, &task.task_id)
}

fn query_oracle_rows(conn: &Connection, task_id: &str) -> Result<Vec<Value>, HarnessError> {
    match task_id {
        "tabdiff_ddxplus_dcr_checkpoint_provenance" => tabdiff_checkpoint_provenance_rows(conn),
        "best_detection_eval_method_audit" => best_detection_eval_method_rows(conn),
        "cancelled_train_interrupt_triage" => cancelled_train_interrupt_rows(conn),
        other => Err(HarnessError::Judge(format!(
            "task {other} has no gold_sql and no file-body oracle"
        ))),
    }
}

fn tabdiff_checkpoint_provenance_rows(conn: &Connection) -> Result<Vec<Value>, HarnessError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT e.experiment_id, a.blob_ref
            FROM experiments e
            JOIN params p ON p.experiment_id = e.experiment_id
                AND p.param_path = 'tabdiff.dataset'
                AND p.value_json = '"ddxplus_dcr"'
            JOIN artifacts a ON a.experiment_id = e.experiment_id
                AND a.artifact_path = 'stdout.txt'
                AND a."exists" = 1
            WHERE e.script_path = 'sample_tabdiff.py'
            ORDER BY e.experiment_id ASC
            "#,
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(from_sql)?;
    let mut matches = Vec::new();
    for row in rows {
        let (experiment_id, blob_ref) = row.map_err(from_sql)?;
        let stdout = sqlite_blob_text(conn, &blob_ref)?;
        let Some(checkpoint_file) = body_anchor_value(&stdout, "Checkpoint: ")
            .map(|value| value.split(" (").next().unwrap_or(value).trim().to_owned())
        else {
            continue;
        };
        let Some(model_parameters) = body_anchor_value(&stdout, "Model parameters: ")
            .and_then(|value| value.trim().replace(',', "").parse::<i64>().ok())
        else {
            continue;
        };
        matches.push(json!({
            "experiment_id": experiment_id,
            "checkpoint_file": checkpoint_file,
            "model_parameters": model_parameters,
        }));
    }
    Ok(matches)
}

fn best_detection_eval_method_rows(conn: &Connection) -> Result<Vec<Value>, HarnessError> {
    let mut stmt = conn
        .prepare(
            r#"
            WITH latest AS (
                SELECT experiment_id, value
                FROM metrics
                WHERE metric_name = 'detection_roc_auc'
                    AND value IS NOT NULL
                    AND value > -9.0e+999
                    AND value < 9.0e+999
                GROUP BY experiment_id
                HAVING step = MAX(step)
            )
            SELECT e.experiment_id, l.value, a.blob_ref
            FROM experiments e
            JOIN latest l ON l.experiment_id = e.experiment_id
            JOIN artifacts a ON a.experiment_id = e.experiment_id
                AND a.artifact_path = 'stdout.txt'
                AND a."exists" = 1
            WHERE e.status = 'completed'
                AND e.script_path = 'eval.py'
            ORDER BY l.value DESC, e.experiment_id ASC
            LIMIT 1
            "#,
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(from_sql)?;
    for row in rows {
        let (experiment_id, detection_roc_auc, blob_ref) = row.map_err(from_sql)?;
        let stdout = sqlite_blob_text(conn, &blob_ref)?;
        let Some(method) = body_anchor_value(&stdout, "Method: ")
            .map(|value| value.trim_end_matches(')').trim().to_owned())
        else {
            continue;
        };
        return Ok(vec![json!({
            "experiment_id": experiment_id,
            "detection_roc_auc": detection_roc_auc,
            "detection_method": method,
        })]);
    }
    Ok(Vec::new())
}

fn cancelled_train_interrupt_rows(conn: &Connection) -> Result<Vec<Value>, HarnessError> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                e.experiment_id,
                e.status,
                a.size_bytes,
                a.blob_ref
            FROM experiments e
            LEFT JOIN artifacts a ON a.experiment_id = e.experiment_id
                AND a.artifact_path = 'stderr.txt'
                AND a."exists" = 1
            WHERE e.status != 'completed'
            ORDER BY e.experiment_id ASC
            "#,
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(from_sql)?;
    let mut matches = Vec::new();
    for row in rows {
        let (experiment_id, status, stderr_size, blob_ref) = row.map_err(from_sql)?;
        let interrupt_line = match blob_ref.as_deref() {
            Some(blob_ref) => {
                let stderr = sqlite_blob_text(conn, blob_ref)?;
                stderr
                    .lines()
                    .enumerate()
                    .filter(|(_, line)| line.contains("KeyboardInterrupt"))
                    .map(|(index, _)| (index + 1) as i64)
                    .last()
            }
            None => None,
        };
        matches.push(json!({
            "experiment_id": experiment_id,
            "status": status,
            "stderr_size_bytes": stderr_size,
            "keyboard_interrupt": interrupt_line.is_some(),
            "interrupt_line_number": interrupt_line,
        }));
    }
    Ok(matches)
}

fn body_anchor_value<'a>(body: &'a str, anchor: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let start = line.find(anchor)? + anchor.len();
        Some(&line[start..])
    })
}

fn sqlite_blob_text(conn: &Connection, blob_ref: &str) -> Result<String, HarnessError> {
    let bytes = query_single_blob(
        conn,
        "SELECT content FROM blobs WHERE blob_ref = ?1",
        blob_ref,
    )?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn expected_run_ids(rows: &[Value], column: Option<&str>) -> Vec<String> {
    let Some(column) = column else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| row.get(column).and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

fn compare_row_arrays(
    expected_rows: &[Value],
    actual_rows: &[Value],
    columns: &[String],
    mismatches: &mut Vec<String>,
) {
    if expected_rows.len() != actual_rows.len() {
        mismatches.push(format!(
            "row count differs; expected {}, got {}",
            expected_rows.len(),
            actual_rows.len()
        ));
        return;
    }
    for (index, (expected, actual)) in expected_rows.iter().zip(actual_rows).enumerate() {
        compare_objects(
            expected,
            actual,
            columns,
            &format!("row {index}"),
            mismatches,
        );
    }
}

fn compare_objects(
    expected: &Value,
    actual: &Value,
    columns: &[String],
    label: &str,
    mismatches: &mut Vec<String>,
) {
    for column in columns {
        let expected_value = expected.get(column).unwrap_or(&Value::Null);
        let actual_value = actual.get(column).unwrap_or(&Value::Null);
        if !values_match(expected_value, actual_value) {
            mismatches.push(format!(
                "{label}.{column} differs; expected {expected_value}, got {actual_value}"
            ));
        }
    }
}

fn values_match(expected: &Value, actual: &Value) -> bool {
    if expected == actual {
        return true;
    }
    match (expected.as_f64(), actual.as_f64()) {
        (Some(left), Some(right)) => {
            let abs = (left - right).abs();
            let rel = abs / left.abs().max(1.0);
            abs <= 1.0e-9 || rel <= 1.0e-6
        }
        _ => matches!(
            (expected.as_i64(), actual.as_bool()),
            (Some(0), Some(false)) | (Some(1), Some(true))
        ),
    }
}

fn judge_index_count_consistency(
    gold_rows: &[Value],
    answer_root: &Value,
    mismatches: &mut Vec<String>,
) -> Result<(), HarnessError> {
    let Some(row) = gold_rows.first() else {
        mismatches.push("gold query returned no index consistency row".to_owned());
        return Ok(());
    };
    let expected_metadata = row
        .get("metadata_completed_count")
        .and_then(sqlite_value_as_i64)
        .ok_or_else(|| HarnessError::Judge("missing metadata_completed_count".to_owned()))?;
    let index_json = row
        .get("completed_index_json")
        .and_then(Value::as_str)
        .ok_or_else(|| HarnessError::Judge("missing completed_index_json".to_owned()))?;
    let index_count = serde_json::from_str::<Value>(index_json)
        .map_err(|err| HarnessError::Json(err.to_string()))?
        .as_array()
        .map(Vec::len)
        .ok_or_else(|| HarnessError::Json("completed index is not a JSON array".to_owned()))?;
    let expected = json!({
        "metadata_completed_count": expected_metadata,
        "index_completed_count": index_count,
        "consistent": expected_metadata == index_count as i64,
    });
    compare_objects(
        &expected,
        answer_root,
        &[
            "metadata_completed_count".to_owned(),
            "index_completed_count".to_owned(),
            "consistent".to_owned(),
        ],
        "answer",
        mismatches,
    );
    Ok(())
}

fn evidence_support(
    conn: &Connection,
    arm: &str,
    answer: &Value,
    nokv_native: Option<&NoKvFs<HoltMetadataStore, S3ObjectStore>>,
) -> Result<(usize, usize), HarnessError> {
    let evidence = answer
        .get("evidence")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("handle").and_then(Value::as_str))
        .collect::<Vec<_>>();
    let mut supported = 0;
    for handle in &evidence {
        if evidence_handle_supported(conn, arm, handle, nokv_native)? {
            supported += 1;
        }
    }
    Ok((evidence.len(), supported))
}

fn evidence_handle_supported(
    conn: &Connection,
    arm: &str,
    handle: &str,
    nokv_native: Option<&NoKvFs<HoltMetadataStore, S3ObjectStore>>,
) -> Result<bool, HarnessError> {
    match arm {
        "sqlite_raw_v1" => sqlite_evidence_supported(conn, handle),
        "nokv_native_v1" => nokv_native_evidence_supported(conn, handle, nokv_native),
        _ => Ok(false),
    }
}

fn sqlite_evidence_supported(conn: &Connection, handle: &str) -> Result<bool, HarnessError> {
    if handle == "sqlite://schema" {
        return Ok(sqlite_count(
            conn,
            "SELECT COUNT(*) FROM sqlite_schema WHERE sql IS NOT NULL",
        )? > 0);
    }
    if let Some(id) = handle.strip_prefix("sqlite://experiments/") {
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM experiments WHERE experiment_id = '{}'",
                escape_sql_literal(id)
            ),
        )? > 0);
    }
    if let Some(blob_ref) = handle.strip_prefix("sqlite://blobs/") {
        let blob_ref = blob_ref.split('#').next().unwrap_or(blob_ref);
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM blobs WHERE blob_ref = '{}'",
                escape_sql_literal(blob_ref)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://artifacts/") {
        let Some((run_id, artifact_path)) = rest.split_once('/') else {
            return Ok(false);
        };
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM artifacts WHERE experiment_id = '{}' AND artifact_path = '{}'",
                escape_sql_literal(run_id),
                escape_sql_literal(artifact_path)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://params/") {
        let Some((run_id, param_path)) = rest.split_once('/') else {
            return Ok(false);
        };
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM params WHERE experiment_id = '{}' AND param_path = '{}'",
                escape_sql_literal(run_id),
                escape_sql_literal(param_path)
            ),
        )? > 0);
    }
    if let Some(rest) = handle.strip_prefix("sqlite://metrics/") {
        let parts = rest.split('/').collect::<Vec<_>>();
        if parts.len() < 3 {
            return Ok(false);
        }
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM metrics WHERE experiment_id = '{}' AND metric_name = '{}' AND step = {}",
                escape_sql_literal(parts[0]),
                escape_sql_literal(parts[1]),
                escape_sql_literal(parts[2])
            ),
        )? > 0);
    }
    if let Some(run_id) = handle.strip_prefix("sqlite://git_state/") {
        return Ok(sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM git_state WHERE experiment_id = '{}'",
                escape_sql_literal(run_id)
            ),
        )? > 0);
    }
    Ok(false)
}

struct NoKvEvidenceHandle {
    path: String,
    generation: Option<u64>,
}

fn nokv_native_evidence_supported<O>(
    conn: &Connection,
    handle: &str,
    service: Option<&NoKvFs<HoltMetadataStore, O>>,
) -> Result<bool, HarnessError>
where
    O: ObjectStore,
{
    let Some(parsed) = parse_nokv_native_evidence_handle(handle) else {
        return Ok(false);
    };
    if let Some(service) = service {
        let Some(metadata) = stat_path_optional(service, &parsed.path)? else {
            return Ok(false);
        };
        return Ok(parsed
            .generation
            .map(|generation| generation == metadata.attr.generation)
            .unwrap_or(true));
    }
    let Some(sqlite_path) = sqlite_path_for_nokv_native_path(&parsed.path) else {
        return Ok(false);
    };
    Ok(sqlite_count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM files WHERE path = '{}'",
            escape_sql_literal(&sqlite_path)
        ),
    )? > 0)
}

fn stat_path_optional<M, O>(
    service: &NoKvFs<M, O>,
    path: &str,
) -> Result<Option<PathMetadata>, HarnessError>
where
    M: MetadataStore,
    O: ObjectStore,
{
    match service.stat_path(path) {
        Ok(metadata) => Ok(metadata),
        Err(MetadError::NotFound) => Ok(None),
        Err(err) => Err(from_nokv(err)),
    }
}

fn parse_nokv_native_evidence_handle(handle: &str) -> Option<NoKvEvidenceHandle> {
    let value = handle.strip_prefix("nokv-native://")?;
    let value = value.split('#').next().unwrap_or(value);
    let (path, generation) = match value.rsplit_once("@generation:") {
        Some((path, generation)) => {
            let generation = generation.parse::<u64>().ok()?;
            if generation == 0 {
                return None;
            }
            (path, Some(generation))
        }
        None => (value, None),
    };
    if !(path == "/yanex" || path.starts_with("/yanex/")) {
        return None;
    }
    Some(NoKvEvidenceHandle {
        path: path.to_owned(),
        generation,
    })
}

fn sqlite_path_for_nokv_native_path(path: &str) -> Option<String> {
    if path == "/yanex" || path == "/yanex/" {
        return Some("/".to_owned());
    }
    if let Some(rest) = path.strip_prefix("/yanex/runs") {
        return Some(if rest.is_empty() {
            "/runs".to_owned()
        } else {
            format!("/runs{rest}")
        });
    }
    if let Some(rest) = path.strip_prefix("/yanex/index") {
        return Some(if rest.is_empty() {
            "/index".to_owned()
        } else {
            format!("/index{rest}")
        });
    }
    None
}

impl BenchmarkRunTelemetry {
    #[cfg(test)]
    fn to_jsonl_line(&self) -> String {
        serde_json::to_string(self).expect("benchmark telemetry serializes")
    }

    #[cfg(test)]
    fn sample_for_test() -> Self {
        Self {
            record_type: "benchmark_run".to_owned(),
            run_id: "unit-run".to_owned(),
            arm_id: "sqlite_raw_v1".to_owned(),
            task_id: "status_counts".to_owned(),
            repeat_index: 0,
            model: "test-model".to_owned(),
            started_at_unix_ms: 1,
            completed_at_unix_ms: 2,
            api_calls: vec![ApiCallTelemetry {
                request_id: Some("req-test".to_owned()),
                response_id: Some("resp-test".to_owned()),
                model: "test-model".to_owned(),
                started_at_unix_ms: 1,
                completed_at_unix_ms: 2,
                previous_response_id: None,
                sent_tool_schema: true,
                sent_initial_instructions: true,
                prompt_tokens: Some(10),
                completion_tokens: Some(5),
                total_tokens: Some(15),
                reasoning_tokens: Some(0),
                cached_prompt_tokens: Some(0),
                accepted_prediction_tokens: Some(0),
                rejected_prediction_tokens: Some(0),
            }],
            tool_calls: vec![ToolCallTelemetry {
                call_id: "call-test".to_owned(),
                tool_name: "query_sql".to_owned(),
                arguments: json!({"sql":"SELECT 1"}),
                started_at_unix_ms: 1,
                completed_at_unix_ms: 2,
                status: "success".to_owned(),
                bytes_read: 0,
                result_token_estimate: 1,
                error: None,
                timed_out: false,
            }],
            derived_metrics: DerivedMetrics {
                tool_call_count: 1,
                task_success: Some(true),
                evidence_precision: None,
                ..DerivedMetrics::default()
            },
            correctness: true,
            judge: Some(JudgeResult {
                task_success: true,
                evidence_precision: None,
                evidence_checked: 0,
                evidence_supported: 0,
                expected_run_ids: vec!["run-1".to_owned()],
                actual_run_ids: vec!["run-1".to_owned()],
                mismatches: Vec::new(),
            }),
            final_answer: Some(
                json!({"status_counts":[{"status":"completed","count":1}],"total_runs":1}),
            ),
            run_error: None,
        }
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn duration_millis(duration: Duration) -> u128 {
    duration.as_millis()
}

fn evidence_refs_for_row(row: &Value) -> Vec<String> {
    let Some(object) = row.as_object() else {
        return Vec::new();
    };
    let experiment_id = object.get("experiment_id").and_then(Value::as_str);
    let mut refs = Vec::new();
    if let Some(experiment_id) = experiment_id {
        push_evidence_ref(&mut refs, format!("sqlite://experiments/{experiment_id}"));
        if let Some(param_path) = object.get("param_path").and_then(Value::as_str) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://params/{experiment_id}/{param_path}"),
            );
        }
        if let Some(metric_name) = object.get("metric_name").and_then(Value::as_str) {
            if let Some(step) = object.get("step").and_then(sqlite_value_as_i64) {
                push_evidence_ref(
                    &mut refs,
                    format!("sqlite://metrics/{experiment_id}/{metric_name}/{step}"),
                );
            }
        }
        if let (Some(dependency_id), Some(relation)) = (
            object
                .get("dependency_experiment_id")
                .and_then(Value::as_str),
            object.get("relation").and_then(Value::as_str),
        ) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://dependencies/{experiment_id}/{dependency_id}/{relation}"),
            );
        }
        if let Some(artifact_path) = object.get("artifact_path").and_then(Value::as_str) {
            push_evidence_ref(
                &mut refs,
                format!("sqlite://artifacts/{experiment_id}/{artifact_path}"),
            );
        }
        if object.contains_key("commit_hash")
            || object.contains_key("dirty")
            || object.contains_key("diff_blob_ref")
        {
            push_evidence_ref(&mut refs, format!("sqlite://git_state/{experiment_id}"));
        }
    }
    if let Some(blob_ref) = object.get("blob_ref").and_then(Value::as_str) {
        push_evidence_ref(&mut refs, format!("sqlite://blobs/{blob_ref}"));
    }
    if let Some(diff_blob_ref) = object.get("diff_blob_ref").and_then(Value::as_str) {
        push_evidence_ref(&mut refs, format!("sqlite://blobs/{diff_blob_ref}"));
    }
    refs
}

fn push_evidence_ref(refs: &mut Vec<String>, value: String) {
    if !refs.contains(&value) {
        refs.push(value);
    }
}

fn sqlite_value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn verify_sqlite_materialization(
    conn: &Connection,
) -> Result<SqliteMaterializationReport, HarnessError> {
    let mut report = SqliteMaterializationReport {
        run_count: sqlite_count(conn, "SELECT COUNT(*) FROM experiments")?,
        ..SqliteMaterializationReport::default()
    };

    let files = sqlite_files(conn)?;
    for (path, bytes) in &files {
        if path.starts_with("/index/") {
            report.index_files_checked += 1;
        } else {
            report.sqlite_files_checked += 1;
        }
        if let Some((run_id, kind, artifact_path)) = parse_sqlite_run_file(path) {
            match kind {
                "metadata.json" => {
                    report.raw_metadata_checked += 1;
                    let raw = query_single_blob(
                        conn,
                        "SELECT CAST(metadata_json AS BLOB) FROM experiments WHERE experiment_id = ?1",
                        run_id,
                    )?;
                    if raw != *bytes {
                        report
                            .mismatches
                            .push(format!("{path}: metadata bytes differ from experiments"));
                    }
                }
                "params.yaml" => {
                    let expected = expected_params(bytes)?;
                    report.raw_params_checked += expected.len();
                    let actual = query_params(conn, run_id)?;
                    compare_maps(&mut report.mismatches, path, "params", &expected, &actual);
                }
                "metrics.json" => {
                    let expected = expected_metrics(Path::new(path), bytes)?;
                    report.raw_metrics_checked += expected.len();
                    let actual = query_metrics(conn, run_id)?;
                    compare_maps(&mut report.mismatches, path, "metrics", &expected, &actual);
                }
                "dependencies.json" => {
                    let expected = expected_dependencies(bytes)?;
                    report.raw_dependencies_checked += expected.len();
                    let actual = query_dependencies(conn, run_id)?;
                    compare_maps(
                        &mut report.mismatches,
                        path,
                        "dependencies",
                        &expected,
                        &actual,
                    );
                }
                "artifacts" => {
                    let Some(artifact_path) = artifact_path else {
                        continue;
                    };
                    report.raw_existing_artifacts_checked += 1;
                    verify_existing_artifact(conn, &mut report, run_id, artifact_path, bytes)?;
                }
                _ => {}
            }
        }
    }

    verify_missing_artifacts(conn, &files, &mut report)?;
    verify_sqlite_agent_index(conn, &mut report)?;
    Ok(report)
}

fn verify_sqlite_agent_index(
    conn: &Connection,
    report: &mut SqliteMaterializationReport,
) -> Result<(), HarnessError> {
    if !sqlite_agent_index_materialization_current(conn)? {
        report
            .mismatches
            .push("run_agent_index materialization is missing or stale".to_owned());
        return Ok(());
    }

    report.agent_index_rows_checked = sqlite_count(conn, "SELECT COUNT(*) FROM run_agent_index")?;
    report.agent_index_values_checked =
        sqlite_count(conn, "SELECT COUNT(*) FROM run_agent_index_values")?;
    report.agent_index_catalog_checked =
        sqlite_count(conn, "SELECT COUNT(*) FROM run_agent_index_catalog")?;

    let missing_rows = sqlite_count(
        conn,
        r#"
        SELECT COUNT(*)
        FROM experiments AS e
        LEFT JOIN run_agent_index AS i ON i.experiment_id = e.experiment_id
        WHERE i.experiment_id IS NULL
        "#,
    )?;
    if missing_rows != 0 {
        report
            .mismatches
            .push(format!("run_agent_index misses {missing_rows} experiments"));
    }
    Ok(())
}

fn verify_nokv_namespace<O>(
    conn: &Connection,
    service: &NoKvFs<HoltMetadataStore, O>,
) -> Result<NamespaceMaterializationReport, HarnessError>
where
    O: ObjectStore,
{
    let files = sqlite_files(conn)?;
    let missing = missing_artifact_paths(conn)?;
    let mut report = NamespaceMaterializationReport::default();
    let mut generations = BTreeSet::new();
    for (sqlite_file_path, bytes) in &files {
        let nokv_path = nokv_path_for_sqlite_file(sqlite_file_path)?;
        let Some(metadata) = stat_path_optional(service, &nokv_path)? else {
            report
                .mismatches
                .push(format!("{nokv_path}: missing NoKV file"));
            continue;
        };
        let body = match metadata.body.as_ref() {
            Some(body) => body,
            None => {
                report
                    .mismatches
                    .push(format!("{nokv_path}: missing body descriptor"));
                continue;
            }
        };
        generations.insert(metadata.attr.generation);
        report.body_descriptors_checked += 1;
        if body.size != bytes.len() as u64 {
            report.mismatches.push(format!(
                "{nokv_path}: body descriptor size {} != expected {}",
                body.size,
                bytes.len()
            ));
        }
        if body.digest_uri != sha256_uri(bytes) {
            report
                .mismatches
                .push(format!("{nokv_path}: body digest differs"));
        }
        let len = usize::try_from(metadata.attr.size)
            .map_err(|_| HarnessError::NoKv(format!("{nokv_path}: file size exceeds usize")))?;
        let read = service
            .read_file(metadata.attr.inode, 0, len)
            .map_err(from_nokv)?;
        if read != *bytes {
            report
                .mismatches
                .push(format!("{nokv_path}: NoKV bytes differ"));
        }
        report.files_checked += 1;
        if sqlite_file_path.starts_with("/index/") {
            report.index_files_checked += 1;
        } else {
            report.run_files_checked += 1;
        }
    }
    for (run_id, artifact_path) in missing {
        let nokv_path = format!("/yanex/runs/{run_id}/artifacts/{artifact_path}");
        if stat_path_optional(service, &nokv_path)?.is_some() {
            report
                .mismatches
                .push(format!("{nokv_path}: missing artifact unexpectedly exists"));
        }
        report.missing_artifacts_checked += 1;
    }
    report.metadata_generations_observed = generations.len();
    verify_nokv_agent_catalog(service, &mut report)?;
    Ok(report)
}

fn verify_nokv_agent_catalog<O>(
    service: &NoKvFs<HoltMetadataStore, O>,
    report: &mut NamespaceMaterializationReport,
) -> Result<(), HarnessError>
where
    O: ObjectStore,
{
    const INDEX_FIELD_PREFIXES: [&str; 6] =
        ["run.", "artifact.", "param.", "metric.", "group.", "git."];
    let expected = nokv_agent_index_fields()
        .iter()
        .map(|field| field.field.id.clone())
        .collect::<BTreeSet<_>>();
    let catalog = execute_agent_tool(
        service,
        "catalog",
        &json!({"path": format!("/{NOKV_PREFIX}/runs"), "include_facets": false}),
    )
    .map_err(from_nokv)?;
    let mut actual = BTreeSet::new();
    for group in catalog["catalog"]["filterable"]
        .as_array()
        .into_iter()
        .flatten()
    {
        for field in group["fields"].as_array().into_iter().flatten() {
            if let Some(id) = field.as_str() {
                actual.insert(id.to_owned());
            }
        }
    }
    for field in &expected {
        report.agent_catalog_fields_checked += 1;
        if !actual.contains(field) {
            report.mismatches.push(format!(
                "agent catalog misses registered index field {field}"
            ));
        }
    }
    for field in &actual {
        if INDEX_FIELD_PREFIXES
            .iter()
            .any(|prefix| field.starts_with(prefix))
            && !expected.contains(field)
        {
            report.mismatches.push(format!(
                "agent catalog exposes stale index field {field}; rerun prepare --reset or nokv-register-indexes"
            ));
        }
    }
    Ok(())
}

fn sqlite_files(conn: &Connection) -> Result<BTreeMap<String, Vec<u8>>, HarnessError> {
    let mut stmt = conn
        .prepare("SELECT path, content FROM files WHERE file_type = 'file' ORDER BY path")
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(from_sql)?;
    let mut files = BTreeMap::new();
    for row in rows {
        let (path, bytes) = row.map_err(from_sql)?;
        files.insert(path, bytes);
    }
    Ok(files)
}

fn parse_sqlite_run_file(path: &str) -> Option<(&str, &str, Option<&str>)> {
    let rest = path.strip_prefix("/runs/")?;
    let (run_id, suffix) = rest.split_once('/')?;
    if let Some(artifact_path) = suffix.strip_prefix("artifacts/") {
        return Some((run_id, "artifacts", Some(artifact_path)));
    }
    Some((run_id, suffix, None))
}

fn nokv_path_for_sqlite_file(path: &str) -> Result<String, HarnessError> {
    if let Some(rest) = path.strip_prefix("/runs/") {
        Ok(format!("/yanex/runs/{rest}"))
    } else if let Some(rest) = path.strip_prefix("/index/") {
        Ok(format!("/yanex/index/{rest}"))
    } else {
        Err(HarnessError::Corpus(format!(
            "unsupported SQLite file path {path}"
        )))
    }
}

fn expected_params(bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let yaml = parse_yaml_bytes(Path::new("params.yaml"), bytes)?;
    let mut flattened = Vec::new();
    flatten_yaml("", &yaml, &mut flattened);
    Ok(flattened
        .into_iter()
        .map(|(path, value)| {
            (
                path,
                format!(
                    "{}\t{}",
                    serde_json::to_string(&yaml_to_json(&value)).expect("yaml json serializes"),
                    yaml_type(&value)
                ),
            )
        })
        .collect())
}

fn query_params(conn: &Connection, run_id: &str) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT param_path, value_json, value_type FROM params WHERE experiment_id = ?1 ORDER BY param_path",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let path: String = row.get(0)?;
            let value_json: String = row.get(1)?;
            let value_type: String = row.get(2)?;
            Ok((path, format!("{value_json}\t{value_type}")))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn expected_metrics(path: &Path, bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let value = parse_metrics_bytes(path, bytes)?;
    let mut expected = BTreeMap::new();
    let Some(records) = value.as_array() else {
        return Ok(expected);
    };
    for (ordinal, record) in records.iter().enumerate() {
        let Some(object) = record.as_object() else {
            continue;
        };
        let step = object
            .get("step")
            .and_then(Value::as_i64)
            .unwrap_or(ordinal as i64);
        let timestamp = object
            .get("timestamp")
            .and_then(Value::as_str)
            .unwrap_or("");
        for (name, value) in object {
            if name == "step" || name == "timestamp" {
                continue;
            }
            expected.insert(
                format!("{name}\t{step}"),
                format!(
                    "{}\t{}\t{}",
                    metric_real(value)
                        .map(|value| value.to_string())
                        .unwrap_or_default(),
                    timestamp,
                    serde_json::to_string(value).expect("metric value serializes")
                ),
            );
        }
    }
    Ok(expected)
}

fn query_metrics(
    conn: &Connection,
    run_id: &str,
) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT metric_name, step, value, timestamp, value_json FROM metrics WHERE experiment_id = ?1 ORDER BY metric_name, step",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let name: String = row.get(0)?;
            let step: i64 = row.get(1)?;
            let value: Option<f64> = row.get(2)?;
            let timestamp: Option<String> = row.get(3)?;
            let value_json: String = row.get(4)?;
            Ok((
                format!("{name}\t{step}"),
                format!(
                    "{}\t{}\t{}",
                    value.map(|value| value.to_string()).unwrap_or_default(),
                    timestamp.unwrap_or_default(),
                    value_json
                ),
            ))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn expected_dependencies(bytes: &[u8]) -> Result<BTreeMap<String, String>, HarnessError> {
    let value = parse_json_bytes(Path::new("dependencies.json"), bytes)?;
    let Some(document) = value.as_object() else {
        return Ok(BTreeMap::new());
    };
    let dependencies = document
        .get("dependencies")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| map.iter());
    let metadata = document.get("metadata").and_then(Value::as_object);
    let mut expected = BTreeMap::new();
    for (relation, dependency) in dependencies {
        let Some(dependency_id) = dependency.as_str() else {
            continue;
        };
        let metadata_json = metadata
            .and_then(|items| items.get(dependency_id))
            .map(|value| serde_json::to_string(value).expect("dependency metadata serializes"))
            .unwrap_or_default();
        let status = metadata
            .and_then(|items| items.get(dependency_id))
            .and_then(|value| value.get("status_at_resolution"))
            .and_then(Value::as_str)
            .unwrap_or("");
        expected.insert(
            format!("{dependency_id}\t{relation}"),
            format!("{status}\t{metadata_json}"),
        );
    }
    Ok(expected)
}

fn query_dependencies(
    conn: &Connection,
    run_id: &str,
) -> Result<BTreeMap<String, String>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT dependency_experiment_id, relation, status_at_resolution, metadata_json FROM dependencies WHERE experiment_id = ?1 ORDER BY dependency_experiment_id, relation",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map(params![run_id], |row| {
            let dependency_id: String = row.get(0)?;
            let relation: String = row.get(1)?;
            let status: Option<String> = row.get(2)?;
            let metadata_json: Option<String> = row.get(3)?;
            Ok((
                format!("{dependency_id}\t{relation}"),
                format!(
                    "{}\t{}",
                    status.unwrap_or_default(),
                    metadata_json.unwrap_or_default()
                ),
            ))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<BTreeMap<_, _>, _>>()
        .map_err(from_sql)
}

fn compare_maps(
    mismatches: &mut Vec<String>,
    path: &str,
    label: &str,
    expected: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) {
    if expected != actual {
        mismatches.push(format!(
            "{path}: {label} differ; expected {} rows, got {} rows",
            expected.len(),
            actual.len()
        ));
    }
}

fn verify_existing_artifact(
    conn: &Connection,
    report: &mut SqliteMaterializationReport,
    run_id: &str,
    artifact_path: &str,
    bytes: &[u8],
) -> Result<(), HarnessError> {
    let row: Option<(i64, i64, String, String)> = conn
        .query_row(
            "SELECT \"exists\", size_bytes, digest, blob_ref FROM artifacts WHERE experiment_id = ?1 AND artifact_path = ?2",
            params![run_id, artifact_path],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(from_sql)?;
    let Some((exists, size_bytes, digest, blob_ref)) = row else {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: missing artifacts row"
        ));
        return Ok(());
    };
    if exists != 1 {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifacts row is not marked existing"
        ));
    }
    if size_bytes != bytes.len() as i64 {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifact size differs"
        ));
    }
    if digest != sha256_uri(bytes) {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: artifact digest differs"
        ));
    }
    let blob = query_single_blob(
        conn,
        "SELECT content FROM blobs WHERE blob_ref = ?1",
        &blob_ref,
    )?;
    if blob != bytes {
        report.mismatches.push(format!(
            "/runs/{run_id}/artifacts/{artifact_path}: blob bytes differ"
        ));
    }
    Ok(())
}

fn verify_missing_artifacts(
    conn: &Connection,
    files: &BTreeMap<String, Vec<u8>>,
    report: &mut SqliteMaterializationReport,
) -> Result<(), HarnessError> {
    for (run_id, artifact_path) in missing_artifact_paths(conn)? {
        report.raw_missing_artifacts_checked += 1;
        let sqlite_file_path = format!("/runs/{run_id}/artifacts/{artifact_path}");
        if files.contains_key(&sqlite_file_path) {
            report.mismatches.push(format!(
                "{sqlite_file_path}: missing artifact exists in SQLite files"
            ));
        }
        let blob_ref = format!("artifact:{run_id}:{artifact_path}");
        let blob_count = sqlite_count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM blobs WHERE blob_ref = '{}'",
                escape_sql_literal(&blob_ref)
            ),
        )?;
        if blob_count != 0 {
            report
                .mismatches
                .push(format!("{blob_ref}: missing artifact has a blob row"));
        }
    }
    Ok(())
}

fn missing_artifact_paths(conn: &Connection) -> Result<Vec<(String, String)>, HarnessError> {
    let mut stmt = conn
        .prepare(
            "SELECT experiment_id, artifact_path FROM artifacts WHERE \"exists\" = 0 ORDER BY experiment_id, artifact_path",
        )
        .map_err(from_sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(from_sql)?;
    rows.collect::<Result<Vec<_>, _>>().map_err(from_sql)
}

fn sqlite_count(conn: &Connection, sql: &str) -> Result<usize, HarnessError> {
    let value: i64 = conn
        .query_row(sql, [], |row| row.get(0))
        .map_err(from_sql)?;
    usize::try_from(value).map_err(|_| HarnessError::Sql(format!("negative count from {sql}")))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn extract_archive(archive: &Path, destination: &Path) -> Result<(), HarnessError> {
    fs::create_dir_all(destination).map_err(from_io)?;
    let file = fs::File::open(archive).map_err(from_io)?;
    let decoder = GzDecoder::new(file);
    Archive::new(decoder).unpack(destination).map_err(from_io)
}

fn load_runs(corpus_root: &Path) -> Result<Vec<CorpusRun>, HarnessError> {
    let experiments_root = corpus_root.join("experiments_leon");
    if !experiments_root.is_dir() {
        return Err(HarnessError::Corpus(format!(
            "expected corpus root {}",
            experiments_root.display()
        )));
    }
    let mut runs = Vec::new();
    for entry in fs::read_dir(&experiments_root).map_err(from_io)? {
        let entry = entry.map_err(from_io)?;
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let id = dir
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| HarnessError::Corpus("run directory is not UTF-8".to_owned()))?
            .to_owned();
        let metadata_path = dir.join("metadata.json");
        if !metadata_path.exists() {
            continue;
        }
        let metadata_bytes = fs::read(&metadata_path).map_err(from_io)?;
        let metadata = parse_json_bytes(&metadata_path, &metadata_bytes)?;
        let params_path = dir.join("params.yaml");
        let params_bytes = read_optional(&params_path)?;
        let params = params_bytes
            .as_ref()
            .map(|bytes| parse_yaml_bytes(&params_path, bytes))
            .transpose()?;
        let metrics_path = dir.join("metrics.json");
        let metrics_bytes = read_optional(&metrics_path)?;
        let metrics = metrics_bytes
            .as_ref()
            .map(|bytes| parse_metrics_bytes(&metrics_path, bytes))
            .transpose()?;
        let dependencies_path = dir.join("dependencies.json");
        let dependencies_bytes = read_optional(&dependencies_path)?;
        let dependencies = dependencies_bytes
            .as_ref()
            .map(|bytes| parse_json_bytes(&dependencies_path, bytes))
            .transpose()?;
        let artifacts = load_artifacts(&dir.join("artifacts"))?;
        runs.push(CorpusRun {
            id,
            metadata_bytes,
            metadata,
            params_bytes,
            params,
            metrics_bytes,
            metrics,
            dependencies_bytes,
            dependencies,
            artifacts,
        });
    }
    runs.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(runs)
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, HarnessError> {
    if path.exists() {
        fs::read(path).map(Some).map_err(from_io)
    } else {
        Ok(None)
    }
}

fn parse_json_bytes(path: &Path, bytes: &[u8]) -> Result<Value, HarnessError> {
    serde_json::from_slice(bytes)
        .map_err(|err| HarnessError::Json(format!("{}: {err}", path.display())))
}

fn parse_yaml_bytes(path: &Path, bytes: &[u8]) -> Result<serde_yaml::Value, HarnessError> {
    serde_yaml::from_slice(bytes)
        .map_err(|err| HarnessError::Yaml(format!("{}: {err}", path.display())))
}

fn parse_metrics_bytes(path: &Path, bytes: &[u8]) -> Result<Value, HarnessError> {
    match serde_json::from_slice(bytes) {
        Ok(value) => Ok(value),
        Err(strict_err) => {
            let text = std::str::from_utf8(bytes).map_err(|err| {
                HarnessError::Json(format!(
                    "{}: {strict_err}; utf8 error: {err}",
                    path.display()
                ))
            })?;
            let Some(sanitized) = sanitize_nonfinite_json_literals(text) else {
                return Err(HarnessError::Json(format!(
                    "{}: {strict_err}",
                    path.display()
                )));
            };
            serde_json::from_str(&sanitized).map_err(|err| {
                HarnessError::Json(format!(
                    "{}: {err}; original strict error: {strict_err}",
                    path.display()
                ))
            })
        }
    }
}

fn sanitize_nonfinite_json_literals(input: &str) -> Option<String> {
    let mut output = String::with_capacity(input.len());
    let mut changed = false;
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;

    while index < input.len() {
        let ch = input[index..]
            .chars()
            .next()
            .expect("index is on a UTF-8 boundary");
        if in_string {
            output.push(ch);
            index += ch.len_utf8();
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            output.push(ch);
            index += ch.len_utf8();
            continue;
        }
        if let Some((literal, len)) = nonfinite_literal_at(input, index) {
            output.push('"');
            output.push_str(literal);
            output.push('"');
            index += len;
            changed = true;
            continue;
        }
        output.push(ch);
        index += ch.len_utf8();
    }

    changed.then_some(output)
}

fn nonfinite_literal_at(input: &str, index: usize) -> Option<(&'static str, usize)> {
    for literal in ["-Infinity", "Infinity", "NaN"] {
        if input[index..].starts_with(literal)
            && is_json_delimiter(input[index + literal.len()..].chars().next())
        {
            return Some((literal, literal.len()));
        }
    }
    None
}

fn is_json_delimiter(ch: Option<char>) -> bool {
    matches!(
        ch,
        None | Some(',' | '}' | ']' | ':' | ' ' | '\n' | '\r' | '\t')
    )
}

fn load_artifacts(root: &Path) -> Result<Vec<CorpusFile>, HarnessError> {
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).into_iter() {
        let entry = entry.map_err(|err| HarnessError::Io(err.to_string()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if is_appledouble(entry.file_name()) {
            continue;
        }
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|err| HarnessError::Io(err.to_string()))?
            .to_string_lossy()
            .replace('\\', "/");
        files.push(CorpusFile {
            relative_path: relative,
            bytes: fs::read(entry.path()).map_err(from_io)?,
        });
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn is_appledouble(name: &OsStr) -> bool {
    name.to_str()
        .map(|value| value.starts_with("._"))
        .unwrap_or(false)
}

fn derive_indexes(runs: &[CorpusRun]) -> Result<DerivedIndexes, HarnessError> {
    let mut summaries = Vec::new();
    for run in runs {
        summaries.push(run_summary(run));
    }
    let mut files = BTreeMap::new();
    let experiments = serde_json::to_vec_pretty(&summaries).expect("index serializes");
    files.insert("/index/experiments.json".to_owned(), experiments);

    let mut by_status = BTreeMap::<String, Vec<RunSummary>>::new();
    let mut by_tag = BTreeMap::<String, Vec<RunSummary>>::new();
    let mut by_script = BTreeMap::<String, Vec<RunSummary>>::new();
    for summary in &summaries {
        if let Some(status) = &summary.status {
            by_status
                .entry(status.clone())
                .or_default()
                .push(summary.clone());
        }
        if let Some(script) = &summary.script {
            by_script
                .entry(script.clone())
                .or_default()
                .push(summary.clone());
        }
        for tag in &summary.tags {
            by_tag.entry(tag.clone()).or_default().push(summary.clone());
        }
    }

    for (status, values) in &by_status {
        files.insert(
            format!("/index/status/{}.json", safe_index_component(status)),
            serde_json::to_vec_pretty(values).expect("status index serializes"),
        );
    }
    for (tag, values) in &by_tag {
        files.insert(
            format!("/index/tags/{}.json", safe_index_component(tag)),
            serde_json::to_vec_pretty(values).expect("tag index serializes"),
        );
    }
    for (script, values) in &by_script {
        files.insert(
            format!("/index/scripts/{}.json", safe_index_component(script)),
            serde_json::to_vec_pretty(values).expect("script index serializes"),
        );
    }

    Ok(DerivedIndexes {
        files,
        status_counts: by_status
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
        tag_counts: by_tag
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
        script_counts: by_script
            .into_iter()
            .map(|(key, values)| (key, values.len()))
            .collect(),
    })
}

fn run_summary(run: &CorpusRun) -> RunSummary {
    let tags = run
        .metadata
        .pointer("/cli_args/tag")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    RunSummary {
        experiment_id: run.id.clone(),
        name: json_str(&run.metadata, "/name"),
        status: json_str(&run.metadata, "/status"),
        project: json_str(&run.metadata, "/project"),
        script: json_str(&run.metadata, "/cli_args/script").map(script_name),
        created_at: json_str(&run.metadata, "/created_at"),
        started_at: json_str(&run.metadata, "/started_at"),
        completed_at: json_str(&run.metadata, "/completed_at"),
        tags,
    }
}

fn prepare_sqlite(
    path: &Path,
    runs: &[CorpusRun],
    indexes: &DerivedIndexes,
) -> Result<(), HarnessError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    remove_if_exists(path)?;
    let mut conn = Connection::open(path).map_err(from_sql)?;
    create_schema(&conn)?;
    let tx = conn.transaction().map_err(from_sql)?;
    for run in runs {
        insert_run(&tx, run)?;
    }
    insert_agent_index_materialization(&tx, runs)?;
    for run in runs {
        insert_sqlite_run_files(&tx, run)?;
    }
    for (path, bytes) in &indexes.files {
        insert_file(&tx, path, "file", bytes)?;
    }
    insert_index_dirs(&tx, indexes)?;
    tx.commit().map_err(from_sql)?;
    Ok(())
}

fn ensure_sqlite_agent_index_materialization(data_root: &Path) -> Result<(), HarnessError> {
    let sqlite_path = sqlite_path(data_root);
    let mut conn = Connection::open(&sqlite_path).map_err(from_sql)?;
    if sqlite_agent_index_materialization_current(&conn)? {
        return Ok(());
    }

    let runs = load_runs(&data_root.join("corpus"))?;
    refresh_sqlite_agent_index_materialization(&mut conn, &runs)
}

fn refresh_sqlite_agent_index_materialization(
    conn: &mut Connection,
    runs: &[CorpusRun],
) -> Result<(), HarnessError> {
    let tx = conn.transaction().map_err(from_sql)?;
    drop_agent_index_schema(&tx)?;
    create_agent_index_schema(&tx)?;
    insert_agent_index_materialization(&tx, runs)?;
    tx.commit().map_err(from_sql)
}

fn sqlite_agent_index_materialization_current(conn: &Connection) -> Result<bool, HarnessError> {
    for table in [
        "run_agent_index",
        "run_agent_index_values",
        "run_agent_index_catalog",
    ] {
        if !sqlite_table_exists(conn, table)? {
            return Ok(false);
        }
    }
    if conn.prepare(RUN_AGENT_INDEX_SCHEMA_PROBE_SQL).is_err() {
        return Ok(false);
    }

    let run_count = sqlite_count(conn, "SELECT COUNT(*) FROM experiments")?;
    let index_count = sqlite_count(conn, "SELECT COUNT(*) FROM run_agent_index")?;
    let catalog_count = sqlite_count(conn, "SELECT COUNT(*) FROM run_agent_index_catalog")?;
    Ok(run_count == index_count && catalog_count == nokv_agent_index_fields().len())
}

fn sqlite_table_exists(conn: &Connection, table: &str) -> Result<bool, HarnessError> {
    let exists: i64 = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
            params![table],
            |row| row.get(0),
        )
        .map_err(from_sql)?;
    Ok(exists != 0)
}

const RUN_AGENT_INDEX_SCHEMA_PROBE_SQL: &str = r#"
    SELECT
        experiment_id,
        run_name,
        run_status,
        run_project,
        run_script,
        artifact_count,
        artifact_stdout_available,
        artifact_stdout_size_bytes,
        artifact_stderr_available,
        artifact_stderr_size_bytes,
        param_origami_training_learning_rate,
        param_origami_training_batch_size,
        metric_val_loss_min,
        metric_utility_tstr_roc_auc_latest,
        metric_utility_trtr_roc_auc_latest,
        metric_fidelity_latest,
        metric_detection_roc_auc_latest,
        metric_privacy_dcr_score_latest,
        group_lr_batch_key,
        group_lr_batch_learning_rate,
        group_lr_batch_batch_size,
        group_lr_batch_representative,
        group_lr_batch_run_count,
        git_patch_file,
        git_patch_declared,
        git_patch_available,
        git_dirty,
        git_has_uncommitted_changes
    FROM run_agent_index
    LIMIT 0
"#;

fn create_schema(conn: &Connection) -> Result<(), HarnessError> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT,
            project TEXT,
            script_path TEXT,
            command TEXT,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            duration REAL,
            tags_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL
        );
        CREATE TABLE params (
            experiment_id TEXT NOT NULL,
            param_path TEXT NOT NULL,
            value_json TEXT NOT NULL,
            value_type TEXT NOT NULL,
            PRIMARY KEY (experiment_id, param_path)
        );
        CREATE TABLE metrics (
            experiment_id TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            step INTEGER,
            value REAL,
            timestamp TEXT,
            value_json TEXT NOT NULL,
            PRIMARY KEY (experiment_id, metric_name, step)
        );
        CREATE TABLE dependencies (
            experiment_id TEXT NOT NULL,
            dependency_experiment_id TEXT NOT NULL,
            relation TEXT NOT NULL,
            status_at_resolution TEXT,
            metadata_json TEXT,
            PRIMARY KEY (experiment_id, dependency_experiment_id, relation)
        );
        CREATE TABLE artifacts (
            experiment_id TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            digest TEXT NOT NULL,
            blob_ref TEXT NOT NULL,
            "exists" INTEGER NOT NULL,
            PRIMARY KEY (experiment_id, artifact_path)
        );
        CREATE TABLE git_state (
            experiment_id TEXT PRIMARY KEY,
            commit_hash TEXT,
            branch TEXT,
            dirty INTEGER,
            patch_file TEXT,
            diff_blob_ref TEXT
        );
        CREATE TABLE blobs (
            blob_ref TEXT PRIMARY KEY,
            size_bytes INTEGER NOT NULL,
            digest TEXT NOT NULL,
            content BLOB NOT NULL
        );
        CREATE TABLE files (
            path TEXT PRIMARY KEY,
            file_type TEXT NOT NULL,
            size_bytes INTEGER,
            digest TEXT,
            content BLOB,
            source TEXT
        );
        CREATE INDEX idx_experiments_status ON experiments(status);
        CREATE INDEX idx_experiments_script ON experiments(script_path);
        CREATE INDEX idx_metrics_name_value ON metrics(metric_name, value);
        CREATE INDEX idx_artifacts_blob_ref ON artifacts(blob_ref);
        "#,
    )
    .map_err(from_sql)?;
    create_agent_index_schema(conn)
}

fn create_agent_index_schema(conn: &Connection) -> Result<(), HarnessError> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS run_agent_index (
            experiment_id TEXT PRIMARY KEY,
            run_name TEXT,
            run_status TEXT,
            run_project TEXT,
            run_script TEXT,
            artifact_count INTEGER NOT NULL,
            artifact_stdout_available INTEGER NOT NULL,
            artifact_stdout_size_bytes INTEGER NOT NULL,
            artifact_stderr_available INTEGER NOT NULL,
            artifact_stderr_size_bytes INTEGER NOT NULL,
            param_origami_training_learning_rate TEXT,
            param_origami_training_batch_size TEXT,
            metric_val_loss_min REAL,
            metric_utility_tstr_roc_auc_latest REAL,
            metric_utility_trtr_roc_auc_latest REAL,
            metric_fidelity_latest REAL,
            metric_detection_roc_auc_latest REAL,
            metric_privacy_dcr_score_latest REAL,
            group_lr_batch_key TEXT,
            group_lr_batch_learning_rate TEXT,
            group_lr_batch_batch_size TEXT,
            group_lr_batch_representative INTEGER,
            group_lr_batch_run_count INTEGER,
            git_patch_file TEXT,
            git_patch_declared INTEGER NOT NULL,
            git_patch_available INTEGER NOT NULL,
            git_dirty INTEGER NOT NULL,
            git_has_uncommitted_changes INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS run_agent_index_values (
            experiment_id TEXT NOT NULL,
            field TEXT NOT NULL,
            value_kind TEXT NOT NULL,
            value_text TEXT NOT NULL,
            value_u64 INTEGER,
            value_f64 REAL,
            PRIMARY KEY (experiment_id, field, value_kind, value_text)
        );
        CREATE TABLE IF NOT EXISTS run_agent_index_catalog (
            field TEXT PRIMARY KEY,
            value_kind TEXT NOT NULL,
            sortable INTEGER NOT NULL,
            facetable INTEGER NOT NULL,
            operators_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_run_agent_index_status_script ON run_agent_index(run_status, run_script);
        CREATE INDEX IF NOT EXISTS idx_run_agent_index_val_loss ON run_agent_index(metric_val_loss_min);
        CREATE INDEX IF NOT EXISTS idx_run_agent_index_values_field_text ON run_agent_index_values(field, value_text);
        CREATE INDEX IF NOT EXISTS idx_run_agent_index_values_field_u64 ON run_agent_index_values(field, value_u64);
        CREATE INDEX IF NOT EXISTS idx_run_agent_index_values_field_f64 ON run_agent_index_values(field, value_f64);
        "#,
    )
    .map_err(from_sql)
}

fn drop_agent_index_schema(conn: &Connection) -> Result<(), HarnessError> {
    conn.execute_batch(
        r#"
        DROP INDEX IF EXISTS idx_run_agent_index_values_field_f64;
        DROP INDEX IF EXISTS idx_run_agent_index_values_field_u64;
        DROP INDEX IF EXISTS idx_run_agent_index_values_field_text;
        DROP INDEX IF EXISTS idx_run_agent_index_val_loss;
        DROP INDEX IF EXISTS idx_run_agent_index_status_script;
        DROP TABLE IF EXISTS run_agent_index_values;
        DROP TABLE IF EXISTS run_agent_index_catalog;
        DROP TABLE IF EXISTS run_agent_index;
        "#,
    )
    .map_err(from_sql)
}

fn insert_run(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let summary = run_summary(run);
    let tags_json = serde_json::to_string(&summary.tags).expect("tags serialize");
    let command = run
        .metadata
        .pointer("/cli_args")
        .map(|value| serde_json::to_string(value).expect("cli args serialize"));
    conn.execute(
        "INSERT INTO experiments VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            run.id,
            summary.name,
            summary.status,
            summary.project,
            summary.script,
            command,
            summary.created_at,
            summary.started_at,
            summary.completed_at,
            run.metadata.pointer("/duration").and_then(Value::as_f64),
            tags_json,
            String::from_utf8_lossy(&run.metadata_bytes).to_string(),
        ],
    )
    .map_err(from_sql)?;
    insert_params(conn, run)?;
    insert_metrics(conn, run)?;
    insert_dependencies(conn, run)?;
    insert_artifacts(conn, run)?;
    insert_git_state(conn, run)?;
    Ok(())
}

fn insert_params(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(params) = &run.params else {
        return Ok(());
    };
    let mut flattened = Vec::new();
    flatten_yaml("", params, &mut flattened);
    for (path, value) in flattened {
        let value_json =
            serde_json::to_string(&yaml_to_json(&value)).expect("yaml json serializes");
        conn.execute(
            "INSERT INTO params VALUES (?1, ?2, ?3, ?4)",
            params![run.id, path, value_json, yaml_type(&value)],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_metrics(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(Value::Array(records)) = &run.metrics else {
        return Ok(());
    };
    for (ordinal, record) in records.iter().enumerate() {
        let Some(object) = record.as_object() else {
            continue;
        };
        let step = object
            .get("step")
            .and_then(Value::as_i64)
            .unwrap_or(ordinal as i64);
        let timestamp = object.get("timestamp").and_then(Value::as_str);
        for (name, value) in object {
            if name == "step" || name == "timestamp" {
                continue;
            }
            conn.execute(
                "INSERT OR REPLACE INTO metrics VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    run.id,
                    name,
                    step,
                    metric_real(value),
                    timestamp,
                    serde_json::to_string(value).expect("metric value serializes")
                ],
            )
            .map_err(from_sql)?;
        }
    }
    Ok(())
}

fn metric_real(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    match value.as_str() {
        Some("Infinity") => Some(f64::INFINITY),
        Some("-Infinity") => Some(f64::NEG_INFINITY),
        Some("NaN") => None,
        _ => None,
    }
}

fn insert_dependencies(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let Some(Value::Object(document)) = &run.dependencies else {
        return Ok(());
    };
    let dependencies = document
        .get("dependencies")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| map.iter());
    let metadata = document.get("metadata").and_then(Value::as_object);
    for (relation, dependency) in dependencies {
        let Some(dependency_id) = dependency.as_str() else {
            continue;
        };
        let metadata_json = metadata
            .and_then(|items| items.get(dependency_id))
            .map(|value| serde_json::to_string(value).expect("dependency metadata serializes"));
        let status = metadata
            .and_then(|items| items.get(dependency_id))
            .and_then(|value| value.get("status_at_resolution"))
            .and_then(Value::as_str);
        conn.execute(
            "INSERT INTO dependencies VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run.id, dependency_id, relation, status, metadata_json],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_artifacts(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let mut existing_paths = BTreeSet::new();
    for artifact in &run.artifacts {
        existing_paths.insert(artifact.relative_path.clone());
        let blob_ref = format!("artifact:{}:{}", run.id, artifact.relative_path);
        let digest = sha256_uri(&artifact.bytes);
        conn.execute(
            "INSERT INTO artifacts VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![
                run.id,
                artifact.relative_path,
                artifact_type(&artifact.relative_path),
                artifact.bytes.len() as i64,
                digest,
                blob_ref,
            ],
        )
        .map_err(from_sql)?;
        conn.execute(
            "INSERT INTO blobs VALUES (?1, ?2, ?3, ?4)",
            params![
                blob_ref,
                artifact.bytes.len() as i64,
                digest,
                artifact.bytes
            ],
        )
        .map_err(from_sql)?;
    }
    for artifact_path in declared_artifact_paths(run) {
        if existing_paths.contains(&artifact_path) {
            continue;
        }
        let blob_ref = format!("artifact:{}:{artifact_path}", run.id);
        conn.execute(
            "INSERT OR IGNORE INTO artifacts VALUES (?1, ?2, ?3, 0, '', ?4, 0)",
            params![
                run.id,
                artifact_path,
                artifact_type(&artifact_path),
                blob_ref,
            ],
        )
        .map_err(from_sql)?;
    }
    Ok(())
}

fn insert_git_state(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    let git = run.metadata.get("git").unwrap_or(&Value::Null);
    let patch_file = git.get("patch_file").and_then(Value::as_str);
    let diff_blob_ref = patch_file.map(|path| format!("artifact:{}:{path}", run.id));
    conn.execute(
        "INSERT INTO git_state VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            run.id,
            git.get("commit_hash").and_then(Value::as_str),
            git.get("branch").and_then(Value::as_str),
            git.get("has_uncommitted_changes")
                .and_then(Value::as_bool)
                .map(|value| if value { 1 } else { 0 }),
            patch_file,
            diff_blob_ref,
        ],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn declared_artifact_paths(run: &CorpusRun) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    if let Some(patch_file) = git_patch_file(run) {
        paths.insert(patch_file);
    }
    paths
}

fn missing_artifact_ref_count(run: &CorpusRun) -> usize {
    let existing = run
        .artifacts
        .iter()
        .map(|artifact| artifact.relative_path.as_str())
        .collect::<BTreeSet<_>>();
    declared_artifact_paths(run)
        .into_iter()
        .filter(|path| !existing.contains(path.as_str()))
        .count()
}

fn git_patch_file(run: &CorpusRun) -> Option<String> {
    run.metadata
        .get("git")
        .and_then(|git| git.get("patch_file"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn insert_sqlite_run_files(conn: &Connection, run: &CorpusRun) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    insert_dir(conn, RUNS_PREFIX)?;
    let run_root = format!("{RUNS_PREFIX}/{}", run.id);
    insert_dir(conn, &run_root)?;
    insert_file(
        conn,
        &format!("{run_root}/metadata.json"),
        "file",
        &run.metadata_bytes,
    )?;
    if let Some(bytes) = &run.params_bytes {
        insert_file(conn, &format!("{run_root}/params.yaml"), "file", bytes)?;
    }
    if let Some(bytes) = &run.metrics_bytes {
        insert_file(conn, &format!("{run_root}/metrics.json"), "file", bytes)?;
    }
    if let Some(bytes) = &run.dependencies_bytes {
        insert_file(
            conn,
            &format!("{run_root}/dependencies.json"),
            "file",
            bytes,
        )?;
    }
    if !run.artifacts.is_empty() {
        insert_dir(conn, &format!("{run_root}/artifacts"))?;
    }
    for artifact in &run.artifacts {
        let path = format!("{run_root}/artifacts/{}", artifact.relative_path);
        insert_parent_dirs(conn, &path)?;
        insert_file(conn, &path, "file", &artifact.bytes)?;
    }
    Ok(())
}

fn insert_index_dirs(conn: &Connection, indexes: &DerivedIndexes) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    insert_dir(conn, "/index")?;
    insert_dir(conn, "/index/status")?;
    insert_dir(conn, "/index/tags")?;
    insert_dir(conn, "/index/scripts")?;
    for path in indexes.files.keys() {
        insert_parent_dirs(conn, path)?;
    }
    Ok(())
}

fn insert_parent_dirs(conn: &Connection, path: &str) -> Result<(), HarnessError> {
    insert_dir(conn, "/")?;
    let mut current = String::new();
    let components = path.trim_matches('/').split('/').collect::<Vec<_>>();
    for component in components.iter().take(components.len().saturating_sub(1)) {
        current.push('/');
        current.push_str(component);
        insert_dir(conn, &current)?;
    }
    Ok(())
}

fn insert_dir(conn: &Connection, path: &str) -> Result<(), HarnessError> {
    conn.execute(
        "INSERT OR IGNORE INTO files(path, file_type, size_bytes, digest, content, source) VALUES (?1, 'directory', NULL, NULL, NULL, 'etl')",
        params![path],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn insert_file(
    conn: &Connection,
    path: &str,
    file_type: &str,
    bytes: &[u8],
) -> Result<(), HarnessError> {
    insert_parent_dirs(conn, path)?;
    conn.execute(
        "INSERT OR REPLACE INTO files VALUES (?1, ?2, ?3, ?4, ?5, 'etl')",
        params![
            path,
            file_type,
            bytes.len() as i64,
            sha256_uri(bytes),
            bytes
        ],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn insert_agent_index_materialization(
    conn: &Connection,
    runs: &[CorpusRun],
) -> Result<(), HarnessError> {
    for field in nokv_agent_index_fields() {
        let operators = field
            .operators
            .iter()
            .map(benchmark_predicate_op_name)
            .collect::<Vec<_>>();
        conn.execute(
            "INSERT OR REPLACE INTO run_agent_index_catalog VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                field.field.id,
                benchmark_index_value_kind(&field),
                i64::from(field.sortable),
                i64::from(field.facetable),
                serde_json::to_string(&operators).expect("operators serialize"),
            ],
        )
        .map_err(from_sql)?;
    }

    let lr_batch_groups = train_lr_batch_groups(runs);
    for run in runs {
        let row = nokv_agent_index_row_with_groups(run, &lr_batch_groups);
        insert_agent_index_row(conn, run, &row)?;
    }
    Ok(())
}

fn insert_agent_index_row(
    conn: &Connection,
    run: &CorpusRun,
    row: &NamespaceIndexRow,
) -> Result<(), HarnessError> {
    for value in &row.values {
        match &value.value {
            NamespacePredicateValue::String(value_text) => {
                conn.execute(
                    "INSERT OR IGNORE INTO run_agent_index_values VALUES (?1, ?2, 'string', ?3, NULL, NULL)",
                    params![run.id, value.field.id, value_text],
                )
                .map_err(from_sql)?;
            }
            NamespacePredicateValue::U64(raw_value) => {
                let value_i64 = u64_to_i64(*raw_value, "run_agent_index_values.value_u64")?;
                conn.execute(
                    "INSERT OR IGNORE INTO run_agent_index_values VALUES (?1, ?2, 'u64', ?3, ?4, NULL)",
                    params![run.id, value.field.id, raw_value.to_string(), value_i64],
                )
                .map_err(from_sql)?;
            }
            NamespacePredicateValue::F64(raw_value) => {
                conn.execute(
                    "INSERT OR IGNORE INTO run_agent_index_values VALUES (?1, ?2, 'f64', ?3, NULL, ?4)",
                    params![run.id, value.field.id, raw_value.to_string(), raw_value],
                )
                .map_err(from_sql)?;
            }
            NamespacePredicateValue::List(_) => {
                return Err(HarnessError::Corpus(
                    "run_agent_index_values does not support list index values".to_owned(),
                ));
            }
        }
    }

    let values = &row.values;
    conn.execute(
        "INSERT OR REPLACE INTO run_agent_index VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28
        )",
        params![
            run.id,
            index_string(values, "run.name"),
            index_string(values, "run.status"),
            index_string(values, "run.project"),
            index_string(values, "run.script"),
            index_u64_i64(values, "artifact.count")?.unwrap_or(0),
            index_u64_i64(values, "artifact.stdout_available")?.unwrap_or(0),
            index_u64_i64(values, "artifact.stdout_size_bytes")?.unwrap_or(0),
            index_u64_i64(values, "artifact.stderr_available")?.unwrap_or(0),
            index_u64_i64(values, "artifact.stderr_size_bytes")?.unwrap_or(0),
            index_string(values, "param.origami.training.learning_rate"),
            index_string(values, "param.origami.training.batch_size"),
            index_f64(values, "metric.val_loss.min"),
            index_f64(values, "metric.utility_tstr_roc_auc.latest"),
            index_f64(values, "metric.utility_trtr_roc_auc.latest"),
            index_f64(values, "metric.fidelity.latest"),
            index_f64(values, "metric.detection_roc_auc.latest"),
            index_f64(values, "metric.privacy_dcr_score.latest"),
            index_string(values, "group.lr_batch.key"),
            index_string(values, "group.lr_batch.learning_rate"),
            index_string(values, "group.lr_batch.batch_size"),
            index_u64_i64(values, "group.lr_batch.representative")?,
            index_u64_i64(values, "group.lr_batch.run_count")?,
            index_string(values, "git.patch_file"),
            index_u64_i64(values, "git.patch_declared")?.unwrap_or(0),
            index_u64_i64(values, "git.patch_available")?.unwrap_or(0),
            index_u64_i64(values, "git.dirty")?.unwrap_or(0),
            index_u64_i64(values, "git.has_uncommitted_changes")?.unwrap_or(0),
        ],
    )
    .map_err(from_sql)?;
    Ok(())
}

fn benchmark_predicate_op_name(op: &NamespacePredicateOp) -> &'static str {
    match op {
        NamespacePredicateOp::Eq => "eq",
        NamespacePredicateOp::NotEqual => "ne",
        NamespacePredicateOp::In => "in",
        NamespacePredicateOp::Prefix => "prefix",
        NamespacePredicateOp::Suffix => "suffix",
        NamespacePredicateOp::Contains => "contains",
        NamespacePredicateOp::GreaterThan => "gt",
        NamespacePredicateOp::GreaterThanOrEqual => "gte",
        NamespacePredicateOp::LessThan => "lt",
        NamespacePredicateOp::LessThanOrEqual => "lte",
        NamespacePredicateOp::Exists => "exists",
        NamespacePredicateOp::NotExists => "not_exists",
    }
}

fn benchmark_index_value_kind(field: &NamespaceIndexField) -> &'static str {
    if field.field.id.starts_with("metric.") {
        return "f64";
    }
    if field.operators.iter().any(|op| {
        matches!(
            op,
            NamespacePredicateOp::GreaterThan
                | NamespacePredicateOp::GreaterThanOrEqual
                | NamespacePredicateOp::LessThan
                | NamespacePredicateOp::LessThanOrEqual
        )
    }) {
        "u64"
    } else {
        "string"
    }
}

fn index_string(values: &[NamespaceIndexValue], field: &str) -> Option<String> {
    values.iter().find_map(|value| {
        (value.field.id == field).then_some(match &value.value {
            NamespacePredicateValue::String(value) => Some(value.clone()),
            NamespacePredicateValue::U64(_)
            | NamespacePredicateValue::F64(_)
            | NamespacePredicateValue::List(_) => None,
        })?
    })
}

fn index_u64_i64(values: &[NamespaceIndexValue], field: &str) -> Result<Option<i64>, HarnessError> {
    values
        .iter()
        .find_map(|value| {
            (value.field.id == field).then_some(match &value.value {
                NamespacePredicateValue::String(_)
                | NamespacePredicateValue::F64(_)
                | NamespacePredicateValue::List(_) => None,
                NamespacePredicateValue::U64(value) => Some(*value),
            })?
        })
        .map(|value| u64_to_i64(value, field))
        .transpose()
}

fn index_f64(values: &[NamespaceIndexValue], field: &str) -> Option<f64> {
    values.iter().find_map(|value| {
        (value.field.id == field).then_some(match &value.value {
            NamespacePredicateValue::F64(value) => Some(*value),
            NamespacePredicateValue::String(_)
            | NamespacePredicateValue::U64(_)
            | NamespacePredicateValue::List(_) => None,
        })?
    })
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| {
        HarnessError::Corpus(format!(
            "agent index field {field} exceeds SQLite integer range: {value}"
        ))
    })
}

struct DirectNoKvBackend<'a, M, O> {
    service: &'a NoKvFs<M, O>,
}

impl<M, O> ArtifactBackend for DirectNoKvBackend<'_, M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    fn lookup_path(&self, absolute_path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        self.service
            .lookup_path(absolute_path)
            .map_err(ClientError::from)
    }

    fn list_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        self.service
            .read_dir_plus_path(absolute_path)
            .map_err(ClientError::from)
    }

    fn list_indexed_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        let mut entries = Vec::new();
        let mut after: Option<DentryName> = None;
        loop {
            let page = self
                .service
                .list_indexed_path_page(absolute_path, after.as_ref(), 1024)
                .map_err(ClientError::from)?;
            entries.extend(page.entries);
            let Some(cursor) = page.next_cursor else {
                break;
            };
            after = Some(cursor);
        }
        Ok(entries)
    }

    fn create_directory_path(
        &self,
        absolute_path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        self.service
            .create_dir_path(absolute_path, mode, uid, gid)
            .map_err(ClientError::from)
    }

    fn publish_new_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        let prepared = self
            .service
            .prepare_artifact_create_path(absolute_path)
            .map_err(ClientError::from)?;
        let session = artifact_publish_session(&prepared, bytes, metadata)?;
        self.service
            .publish_prepared_artifact_session(prepared, session)
            .map(|result| result.entry)
            .map_err(ClientError::from)
    }

    fn replace_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        let prepared = self
            .service
            .prepare_artifact_replace_path(absolute_path)
            .map_err(ClientError::from)?;
        let session = artifact_publish_session(&prepared, bytes, metadata)?;
        self.service
            .publish_prepared_artifact_session(prepared, session)
            .map_err(ClientError::from)
    }

    fn read_file_path(&self, absolute_path: &str) -> Result<Vec<u8>, ClientError> {
        let entry = self
            .service
            .lookup_path(absolute_path)
            .map_err(ClientError::from)?
            .ok_or_else(|| ClientError::NotFound(absolute_path.to_owned()))?;
        if entry.attr.file_type != FileType::File {
            return Err(ClientError::Metadata(MetadError::NotFile));
        }
        let len = usize::try_from(entry.attr.size).map_err(|_| {
            ClientError::Protocol(format!(
                "artifact {absolute_path} size exceeds platform limit"
            ))
        })?;
        self.service
            .read_file(entry.attr.inode, 0, len)
            .map_err(ClientError::from)
    }

    fn remove_file_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        self.service
            .remove_file_path(absolute_path)
            .map_err(ClientError::from)
    }

    fn remove_empty_dir_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        self.service
            .remove_empty_dir_path(absolute_path)
            .map_err(ClientError::from)
    }
}

fn artifact_publish_session(
    prepared: &PreparedArtifact,
    bytes: Vec<u8>,
    metadata: ArtifactMetadata,
) -> Result<PublishArtifactSession, ClientError> {
    let size = u64::try_from(bytes.len())
        .map_err(|_| ClientError::Protocol("artifact size exceeds u64".to_owned()))?;
    Ok(PublishArtifactSession {
        parent: prepared.parent,
        name: prepared.name.clone(),
        producer: metadata.producer,
        digest_uri: metadata.digest_uri,
        content_type: metadata.content_type,
        manifest_id: metadata.manifest_id,
        size,
        ranges: vec![PublishArtifactRange { offset: 0, bytes }],
        mode: metadata.mode,
        uid: metadata.uid,
        gid: metadata.gid,
    })
}

fn prepare_nokv(
    meta_path: &Path,
    s3: &S3Options,
    runs: &[CorpusRun],
    indexes: &DerivedIndexes,
) -> Result<(), HarnessError> {
    if let Some(parent) = meta_path.parent() {
        fs::create_dir_all(parent).map_err(from_io)?;
    }
    fs::create_dir_all(meta_path).map_err(from_io)?;
    let metadata = HoltMetadataStore::open_file(meta_path).map_err(from_nokv)?;
    let objects = S3ObjectStore::new(s3_options(s3)).map_err(from_nokv)?;
    let service = NoKvFs::new(
        MountId::new(1).expect("mount id is non-zero"),
        metadata,
        objects,
    );
    service
        .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
        .map_err(from_nokv)?;
    let repo = ArtifactRepository::with_options(
        DirectNoKvBackend { service: &service },
        ArtifactRepositoryOptions {
            producer: "yanex-agent-benchmark-etl".to_owned(),
            content_type: "application/octet-stream".to_owned(),
            ..ArtifactRepositoryOptions::default()
        },
    );
    for run in runs {
        repo.put_bytes(
            &format!("{NOKV_PREFIX}/runs/{}/metadata.json", run.id),
            run.metadata_bytes.clone(),
        )
        .map_err(from_nokv)?;
        if let Some(bytes) = &run.params_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/params.yaml", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        if let Some(bytes) = &run.metrics_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/metrics.json", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        if let Some(bytes) = &run.dependencies_bytes {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/dependencies.json", run.id),
                bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
        for artifact in &run.artifacts {
            repo.put_bytes(
                &format!(
                    "{NOKV_PREFIX}/runs/{}/artifacts/{}",
                    run.id, artifact.relative_path
                ),
                artifact.bytes.clone(),
            )
            .map_err(from_nokv)?;
        }
    }
    for (path, bytes) in &indexes.files {
        repo.put_bytes(&format!("{NOKV_PREFIX}{}", path), bytes.clone())
            .map_err(from_nokv)?;
    }
    register_nokv_agent_indexes(&service, runs)?;
    Ok(())
}

fn register_nokv_agent_indexes<O>(
    service: &NoKvFs<HoltMetadataStore, O>,
    runs: &[CorpusRun],
) -> Result<(), HarnessError>
where
    O: ObjectStore,
{
    let lr_batch_groups = train_lr_batch_groups(runs);
    service
        .register_namespace_index(NamespaceIndexRegistration {
            path: format!("/{NOKV_PREFIX}/runs"),
            fields: nokv_agent_index_fields(),
            rows: runs
                .iter()
                .map(|run| nokv_agent_index_row_with_groups(run, &lr_batch_groups))
                .collect(),
        })
        .map_err(from_nokv)
}

fn nokv_agent_index_fields() -> Vec<NamespaceIndexField> {
    vec![
        string_index_field("run.id", true, true),
        string_index_field("run.name", true, true),
        string_index_field("run.status", true, true),
        string_index_field("run.project", true, true),
        string_index_field("run.script", true, true),
        string_index_field("run.tag", true, false),
        string_index_field("artifact.path", true, true),
        string_index_field("artifact.name", true, true),
        string_index_field("artifact.type", true, true),
        u64_index_field("artifact.count", true, true),
        u64_index_field("artifact.size_bytes", false, true),
        u64_index_field("artifact.stdout_available", true, true),
        u64_index_field("artifact.stdout_size_bytes", false, true),
        u64_index_field("artifact.stderr_available", true, true),
        u64_index_field("artifact.stderr_size_bytes", false, true),
        string_index_field("param.origami.training.learning_rate", true, true),
        string_index_field("param.origami.training.batch_size", true, true),
        f64_index_field("metric.val_loss.min", false, true),
        f64_index_field("metric.utility_tstr_roc_auc.latest", false, true),
        f64_index_field("metric.utility_trtr_roc_auc.latest", false, true),
        f64_index_field("metric.fidelity.latest", false, true),
        f64_index_field("metric.detection_roc_auc.latest", false, true),
        f64_index_field("metric.privacy_dcr_score.latest", false, true),
        string_index_field("group.lr_batch.key", true, true),
        string_index_field("group.lr_batch.learning_rate", true, true),
        string_index_field("group.lr_batch.batch_size", true, true),
        u64_index_field("group.lr_batch.representative", true, true),
        u64_index_field("group.lr_batch.run_count", true, true),
        string_index_field("git.patch_file", true, true),
        u64_index_field("git.patch_declared", true, true),
        u64_index_field("git.patch_available", true, true),
        u64_index_field("git.dirty", true, true),
        u64_index_field("git.has_uncommitted_changes", true, true),
    ]
}

#[cfg(test)]
fn nokv_agent_index_row(run: &CorpusRun) -> NamespaceIndexRow {
    nokv_agent_index_row_with_groups(run, &BTreeMap::new())
}

fn nokv_agent_index_row_with_groups(
    run: &CorpusRun,
    lr_batch_groups: &BTreeMap<String, LrBatchGroupSummary>,
) -> NamespaceIndexRow {
    let summary = run_summary(run);
    let mut values = Vec::new();
    push_index_string(&mut values, "run.id", &run.id);
    push_index_optional_string(&mut values, "run.name", summary.name.as_deref());
    push_index_optional_string(&mut values, "run.status", summary.status.as_deref());
    push_index_optional_string(&mut values, "run.project", summary.project.as_deref());
    push_index_optional_string(&mut values, "run.script", summary.script.as_deref());
    for tag in summary.tags {
        push_index_string(&mut values, "run.tag", &tag);
    }

    let mut stdout_size = None;
    let mut stderr_size = None;
    let mut artifact_paths = BTreeSet::new();
    for artifact in &run.artifacts {
        artifact_paths.insert(artifact.relative_path.clone());
        push_index_string(&mut values, "artifact.path", &artifact.relative_path);
        push_index_string(
            &mut values,
            "artifact.name",
            artifact_name(&artifact.relative_path),
        );
        push_index_string(
            &mut values,
            "artifact.type",
            artifact_type(&artifact.relative_path),
        );
        push_index_u64(
            &mut values,
            "artifact.size_bytes",
            artifact.bytes.len() as u64,
        );
        match artifact_name(&artifact.relative_path) {
            "stdout.txt" => stdout_size = Some(artifact.bytes.len() as u64),
            "stderr.txt" => stderr_size = Some(artifact.bytes.len() as u64),
            _ => {}
        }
    }
    push_index_u64(&mut values, "artifact.count", run.artifacts.len() as u64);
    push_index_u64(
        &mut values,
        "artifact.stdout_available",
        u64::from(stdout_size.is_some()),
    );
    push_index_u64(
        &mut values,
        "artifact.stdout_size_bytes",
        stdout_size.unwrap_or(0),
    );
    push_index_u64(
        &mut values,
        "artifact.stderr_available",
        u64::from(stderr_size.is_some()),
    );
    push_index_u64(
        &mut values,
        "artifact.stderr_size_bytes",
        stderr_size.unwrap_or(0),
    );

    push_param_index_values(&mut values, run);
    push_metric_index_values(&mut values, run);
    push_lr_batch_group_values(&mut values, run, lr_batch_groups);

    let patch_file = git_patch_file(run);
    let patch_available = patch_file
        .as_ref()
        .map(|path| artifact_paths.contains(path))
        .unwrap_or(false);
    push_index_optional_string(&mut values, "git.patch_file", patch_file.as_deref());
    push_index_u64(
        &mut values,
        "git.patch_declared",
        u64::from(patch_file.is_some()),
    );
    push_index_u64(
        &mut values,
        "git.patch_available",
        u64::from(patch_available),
    );
    let git_dirty = run
        .metadata
        .get("git")
        .and_then(|git| git.get("has_uncommitted_changes"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    push_index_u64(&mut values, "git.dirty", u64::from(git_dirty));
    push_index_u64(
        &mut values,
        "git.has_uncommitted_changes",
        u64::from(git_dirty),
    );

    NamespaceIndexRow {
        path: format!("/{NOKV_PREFIX}/runs/{}", run.id),
        values,
    }
}

fn train_lr_batch_groups(runs: &[CorpusRun]) -> BTreeMap<String, LrBatchGroupSummary> {
    let mut groups = BTreeMap::<LrBatchKey, Vec<(String, f64)>>::new();
    for run in runs {
        let summary = run_summary(run);
        if summary.status.as_deref() != Some("completed")
            || summary.script.as_deref() != Some("train.py")
        {
            continue;
        }
        let Some(key) = lr_batch_key(run) else {
            continue;
        };
        let Some(min_val_loss) = metric_min(run, "val_loss") else {
            continue;
        };
        groups
            .entry(key)
            .or_default()
            .push((run.id.clone(), min_val_loss));
    }
    groups
        .into_iter()
        .map(|(key, mut values)| {
            values.sort_by(|left, right| left.0.cmp(&right.0));
            let run_count = values.len();
            let representative_run_id = values
                .first()
                .map(|(run_id, _)| run_id.clone())
                .unwrap_or_default();
            (
                lr_batch_key_string(&key),
                LrBatchGroupSummary {
                    key,
                    run_count,
                    representative_run_id,
                },
            )
        })
        .collect()
}

fn push_lr_batch_group_values(
    values: &mut Vec<NamespaceIndexValue>,
    run: &CorpusRun,
    groups: &BTreeMap<String, LrBatchGroupSummary>,
) {
    let Some(key) = lr_batch_key(run) else {
        return;
    };
    let key_string = lr_batch_key_string(&key);
    let Some(group) = groups.get(&key_string) else {
        return;
    };
    push_index_string(values, "group.lr_batch.key", &key_string);
    push_index_string(
        values,
        "group.lr_batch.learning_rate",
        &group.key.learning_rate,
    );
    push_index_string(values, "group.lr_batch.batch_size", &group.key.batch_size);
    push_index_u64(
        values,
        "group.lr_batch.representative",
        u64::from(run.id == group.representative_run_id),
    );
    push_index_u64(values, "group.lr_batch.run_count", group.run_count as u64);
}

fn lr_batch_key(run: &CorpusRun) -> Option<LrBatchKey> {
    Some(LrBatchKey {
        learning_rate: param_index_value(run, "origami.training.learning_rate")?,
        batch_size: param_index_value(run, "origami.training.batch_size")?,
    })
}

fn lr_batch_key_string(key: &LrBatchKey) -> String {
    format!("lr={};batch={}", key.learning_rate, key.batch_size)
}

fn push_param_index_values(values: &mut Vec<NamespaceIndexValue>, run: &CorpusRun) {
    for (param_path, field_id) in PARAM_INDEX_FIELDS {
        let Some(value_json) = param_index_value(run, param_path) else {
            continue;
        };
        push_index_string(values, field_id, &value_json);
    }
}

fn param_index_value(run: &CorpusRun, param_path: &str) -> Option<String> {
    let params = run.params.as_ref()?;
    let mut flattened = Vec::new();
    flatten_yaml("", params, &mut flattened);
    let flattened = flattened.into_iter().collect::<BTreeMap<_, _>>();
    flattened
        .get(param_path)
        .map(|value| serde_json::to_string(&yaml_to_json(value)).expect("yaml json serializes"))
}

fn push_metric_index_values(values: &mut Vec<NamespaceIndexValue>, run: &CorpusRun) {
    if let Some(metric) = metric_min(run, "val_loss") {
        push_metric_value(values, "metric.val_loss.min", metric);
    }
    for metric_name in LATEST_METRIC_INDEX_FIELDS {
        if let Some(metric) = metric_latest(run, metric_name) {
            push_metric_value(values, &format!("metric.{metric_name}.latest"), metric);
        }
    }
}

fn metric_min(run: &CorpusRun, metric_name: &str) -> Option<f64> {
    metric_records(run)
        .filter_map(|record| record.get(metric_name).and_then(metric_real))
        .filter(|value| value.is_finite())
        .min_by(|left, right| left.total_cmp(right))
}

fn metric_latest(run: &CorpusRun, metric_name: &str) -> Option<f64> {
    metric_records(run)
        .filter_map(|record| {
            let step = record.get("step").and_then(Value::as_i64).unwrap_or(0);
            let value = record.get(metric_name).and_then(metric_real)?;
            value.is_finite().then_some((step, value))
        })
        .max_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, value)| value)
}

fn metric_records(run: &CorpusRun) -> impl Iterator<Item = &serde_json::Map<String, Value>> {
    run.metrics
        .as_ref()
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
}

fn push_metric_value(values: &mut Vec<NamespaceIndexValue>, field: &str, value: f64) {
    if value.is_finite() {
        values.push(NamespaceIndexValue {
            field: NamespaceFindField::new(field),
            value: NamespacePredicateValue::F64(value),
        });
    }
}

fn string_index_field(id: &'static str, facetable: bool, sortable: bool) -> NamespaceIndexField {
    NamespaceIndexField {
        field: NamespaceFindField::new(id),
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::NotEqual,
            NamespacePredicateOp::In,
            NamespacePredicateOp::Prefix,
            NamespacePredicateOp::Suffix,
            NamespacePredicateOp::Contains,
            NamespacePredicateOp::Exists,
            NamespacePredicateOp::NotExists,
        ],
        sortable,
        facetable,
    }
}

fn u64_index_field(id: &'static str, facetable: bool, sortable: bool) -> NamespaceIndexField {
    NamespaceIndexField {
        field: NamespaceFindField::new(id),
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::NotEqual,
            NamespacePredicateOp::In,
            NamespacePredicateOp::GreaterThan,
            NamespacePredicateOp::GreaterThanOrEqual,
            NamespacePredicateOp::LessThan,
            NamespacePredicateOp::LessThanOrEqual,
            NamespacePredicateOp::Exists,
            NamespacePredicateOp::NotExists,
        ],
        sortable,
        facetable,
    }
}

fn f64_index_field(id: &'static str, facetable: bool, sortable: bool) -> NamespaceIndexField {
    NamespaceIndexField {
        field: NamespaceFindField::new(id),
        operators: vec![
            NamespacePredicateOp::Eq,
            NamespacePredicateOp::NotEqual,
            NamespacePredicateOp::In,
            NamespacePredicateOp::GreaterThan,
            NamespacePredicateOp::GreaterThanOrEqual,
            NamespacePredicateOp::LessThan,
            NamespacePredicateOp::LessThanOrEqual,
            NamespacePredicateOp::Exists,
            NamespacePredicateOp::NotExists,
        ],
        sortable,
        facetable,
    }
}

fn push_index_optional_string(
    values: &mut Vec<NamespaceIndexValue>,
    field: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        push_index_string(values, field, value);
    }
}

fn push_index_string(values: &mut Vec<NamespaceIndexValue>, field: &str, value: &str) {
    if value.is_empty() {
        return;
    }
    values.push(NamespaceIndexValue {
        field: NamespaceFindField::new(field),
        value: NamespacePredicateValue::String(value.to_owned()),
    });
}

fn push_index_u64(values: &mut Vec<NamespaceIndexValue>, field: &str, value: u64) {
    values.push(NamespaceIndexValue {
        field: NamespaceFindField::new(field),
        value: NamespacePredicateValue::U64(value),
    });
}

fn artifact_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn nokv_register_indexes(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let runs = load_runs(&data_root.join("corpus"))?;
    let service = open_existing_nokv(&nokv_meta_path(&data_root), &options.s3)?;
    register_nokv_agent_indexes(&service, &runs)?;
    print_json(&json!({
        "status": "completed",
        "run_count": runs.len(),
        "indexed_path": format!("/{NOKV_PREFIX}/runs"),
    }))
}

fn agent_tool_debug(options: Options) -> Result<(), HarnessError> {
    let data_root = required_data_root(&options)?;
    let arm = options
        .arm
        .clone()
        .ok_or_else(|| HarnessError::Corpus("agent-tool requires --arm".to_owned()))?;
    let tool_name = options
        .tool_name
        .clone()
        .ok_or_else(|| HarnessError::Corpus("agent-tool requires --tool-name".to_owned()))?;
    let args: Value = serde_json::from_str(options.args_json.as_deref().unwrap_or("{}"))
        .map_err(|err| HarnessError::Json(err.to_string()))?;
    let mut runtime = ArmRuntime::open(&arm, &data_root, &options.s3)?;
    let result = match &mut runtime {
        ArmRuntime::SqliteRaw { conn } => execute_sqlite_raw_tool(conn, &tool_name, &args)?,
        ArmRuntime::NokvNative { service } => {
            execute_nokv_tool(service.as_ref(), &tool_name, &args)?
        }
    };
    let compact =
        serde_json::to_string(&result).map_err(|err| HarnessError::Json(err.to_string()))?;
    eprintln!(
        "tool={tool_name} arm={arm} result_bytes={} approx_tokens={}",
        compact.len(),
        compact.len() / 4
    );
    print_json(&result)
}

fn open_existing_nokv(
    meta_path: &Path,
    s3: &S3Options,
) -> Result<NoKvFs<HoltMetadataStore, S3ObjectStore>, HarnessError> {
    let metadata = HoltMetadataStore::open_file(meta_path).map_err(from_nokv)?;
    let objects = S3ObjectStore::new(s3_options(s3)).map_err(from_nokv)?;
    NoKvFs::open_existing(
        MountId::new(1).expect("mount id is non-zero"),
        metadata,
        objects,
        0,
    )
    .map_err(from_nokv)
}

fn s3_options(s3: &S3Options) -> S3ObjectStoreOptions {
    S3ObjectStoreOptions::rustfs(
        s3.bucket.clone(),
        s3.endpoint.clone(),
        s3.access_key_id.clone(),
        s3.secret_access_key.clone(),
    )
}

fn sqlite_path(data_root: &Path) -> PathBuf {
    data_root.join("sqlite").join("yanex.db")
}

fn nokv_meta_path(data_root: &Path) -> PathBuf {
    data_root.join("nokv").join("meta")
}

fn first_experiment_id(conn: &Connection) -> Result<String, HarnessError> {
    conn.query_row(
        "SELECT experiment_id FROM experiments ORDER BY experiment_id LIMIT 1",
        [],
        |row| row.get(0),
    )
    .map_err(from_sql)
}

fn query_single_blob(conn: &Connection, sql: &str, value: &str) -> Result<Vec<u8>, HarnessError> {
    conn.query_row(sql, params![value], |row| row.get::<_, Vec<u8>>(0))
        .map_err(from_sql)
}

fn sqlite_value(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Value> {
    let value = row.get_ref(index)?;
    Ok(match value {
        rusqlite::types::ValueRef::Null => Value::Null,
        rusqlite::types::ValueRef::Integer(value) => json!(value),
        rusqlite::types::ValueRef::Real(value) => json!(value),
        rusqlite::types::ValueRef::Text(value) => {
            Value::String(String::from_utf8_lossy(value).to_string())
        }
        rusqlite::types::ValueRef::Blob(value) => json!({
            "blob_bytes": value.len(),
            "sha256": sha256_uri(value),
        }),
    })
}

fn flatten_yaml(
    prefix: &str,
    value: &serde_yaml::Value,
    out: &mut Vec<(String, serde_yaml::Value)>,
) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (key, child) in map {
                let Some(key) = key.as_str() else {
                    continue;
                };
                let path = if prefix.is_empty() {
                    key.to_owned()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_yaml(&path, child, out);
            }
        }
        _ => out.push((prefix.to_owned(), value.clone())),
    }
}

fn yaml_to_json(value: &serde_yaml::Value) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn yaml_type(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "null",
        serde_yaml::Value::Bool(_) => "boolean",
        serde_yaml::Value::Number(_) => "number",
        serde_yaml::Value::String(_) => "string",
        serde_yaml::Value::Sequence(_) => "array",
        serde_yaml::Value::Mapping(_) => "object",
        serde_yaml::Value::Tagged(_) => "tagged",
    }
}

fn json_str(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn script_name(value: String) -> String {
    Path::new(&value)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or(&value)
        .to_owned()
}

fn safe_index_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn artifact_type(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(OsStr::to_str) {
        Some("json") => "json",
        Some("txt") => "text",
        Some("patch") => "patch",
        Some("yaml") | Some("yml") => "yaml",
        _ => "binary",
    }
}

fn sha256_uri(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("hex writes to string");
    }
    output
}

fn file_sha256(path: &Path) -> Result<String, HarnessError> {
    let mut file = fs::File::open(path).map_err(from_io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(from_io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn remove_if_exists(path: &Path) -> Result<(), HarnessError> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path).map_err(from_io)
    } else {
        fs::remove_file(path).map_err(from_io)
    }
}

fn value<'a>(
    args: &'a [String],
    index: usize,
    option: &'static str,
) -> Result<&'a str, HarnessError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(HarnessError::MissingOption(option))
}

fn parse_u64(raw: &str, option: &'static str) -> Result<u64, HarnessError> {
    raw.parse::<u64>().map_err(|_| HarnessError::InvalidNumber {
        option,
        value: raw.to_owned(),
    })
}

fn parse_usize(raw: &str, option: &'static str) -> Result<usize, HarnessError> {
    raw.parse::<usize>()
        .map_err(|_| HarnessError::InvalidNumber {
            option,
            value: raw.to_owned(),
        })
}

fn from_io(err: impl Error) -> HarnessError {
    HarnessError::Io(err.to_string())
}

fn from_sql(err: impl Error) -> HarnessError {
    HarnessError::Sql(err.to_string())
}

fn from_nokv(err: impl Error) -> HarnessError {
    HarnessError::NoKv(err.to_string())
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCommand => write!(f, "missing command"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::MissingOption(option) => write!(f, "{option} is required"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::InvalidNumber { option, value } => {
                write!(f, "{option} expects a valid number, got {value}")
            }
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Json(err) => write!(f, "json error: {err}"),
            Self::Yaml(err) => write!(f, "yaml error: {err}"),
            Self::Sql(err) => write!(f, "sqlite error: {err}"),
            Self::NoKv(err) => write!(f, "nokv error: {err}"),
            Self::Corpus(err) => write!(f, "corpus error: {err}"),
            Self::Judge(err) => write!(f, "judge error: {err}"),
            Self::Http(err) => write!(f, "http error: {err}"),
            Self::ToolTimeout { tool, limit_ms } => {
                write!(f, "tool {tool} timed out after {limit_ms}ms")
            }
        }
    }
}

impl Error for HarnessError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct TestEnvVar {
        key: &'static str,
        old_value: Option<std::ffi::OsString>,
    }

    impl TestEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let old_value = env::var_os(key);
            env::set_var(key, value);
            Self { key, old_value }
        }
    }

    impl Drop for TestEnvVar {
        fn drop(&mut self) {
            if let Some(value) = &self.old_value {
                env::set_var(self.key, value);
            } else {
                env::remove_var(self.key);
            }
        }
    }

    fn sample_run() -> CorpusRun {
        let metadata_bytes = br#"{
          "id":"run-1",
          "name":"demo",
          "status":"completed",
          "project":"origami",
          "created_at":"2026-01-01T00:00:00",
          "started_at":"2026-01-01T00:00:01",
          "completed_at":"2026-01-01T00:00:02",
          "duration":1.0,
          "cli_args":{"script":"scripts/train.py","tag":["sweep","final"]}
        }"#
        .to_vec();
        CorpusRun {
            id: "run-1".to_owned(),
            metadata: serde_json::from_slice(&metadata_bytes).unwrap(),
            metadata_bytes,
            params_bytes: Some(
                b"alpha: 1\nnested:\n  beta: true\norigami:\n  training:\n    learning_rate: 0.001\n    batch_size: 32\n"
                    .to_vec(),
            ),
            params: Some(
                serde_yaml::from_slice(
                    b"alpha: 1\nnested:\n  beta: true\norigami:\n  training:\n    learning_rate: 0.001\n    batch_size: 32\n",
                )
                .unwrap(),
            ),
            metrics_bytes: Some(
                br#"[{"step":0,"accuracy":0.9,"val_loss":0.4,"timestamp":"now"},{"step":1,"val_loss":0.3,"utility_tstr_roc_auc":0.8}]"#
                    .to_vec(),
            ),
            metrics: Some(
                serde_json::from_slice(
                    br#"[{"step":0,"accuracy":0.9,"val_loss":0.4,"timestamp":"now"},{"step":1,"val_loss":0.3,"utility_tstr_roc_auc":0.8}]"#,
                )
                .unwrap(),
            ),
            dependencies_bytes: Some(br#"{"dependencies":{"model":"base"},"metadata":{"base":{"status_at_resolution":"completed"}}}"#.to_vec()),
            dependencies: Some(serde_json::from_slice(br#"{"dependencies":{"model":"base"},"metadata":{"base":{"status_at_resolution":"completed"}}}"#).unwrap()),
            artifacts: vec![CorpusFile {
                relative_path: "stdout.txt".to_owned(),
                bytes: b"log".to_vec(),
            }],
        }
    }

    fn write_sample_corpus(data_root: &Path, run: &CorpusRun) {
        let run_dir = data_root
            .join("corpus")
            .join("experiments_leon")
            .join(&run.id);
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(run_dir.join("metadata.json"), &run.metadata_bytes).unwrap();
        if let Some(bytes) = &run.params_bytes {
            fs::write(run_dir.join("params.yaml"), bytes).unwrap();
        }
        if let Some(bytes) = &run.metrics_bytes {
            fs::write(run_dir.join("metrics.json"), bytes).unwrap();
        }
        if let Some(bytes) = &run.dependencies_bytes {
            fs::write(run_dir.join("dependencies.json"), bytes).unwrap();
        }
        if !run.artifacts.is_empty() {
            let artifacts_dir = run_dir.join("artifacts");
            fs::create_dir_all(&artifacts_dir).unwrap();
            for artifact in &run.artifacts {
                fs::write(artifacts_dir.join(&artifact.relative_path), &artifact.bytes).unwrap();
            }
        }
    }

    fn prepare_sample_sqlite_data_root(data_root: &Path) {
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        prepare_sqlite(&sqlite_path(data_root), &[run], &indexes).unwrap();
    }

    fn successful_empty_runs_answer() -> Value {
        json!({
            "runs": []
        })
    }

    fn start_response_capture_server(
        expected_requests: usize,
        final_answer: Value,
    ) -> (
        String,
        Arc<std::sync::Mutex<Vec<Value>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let answer_content = final_answer.to_string();
        let handle = thread::spawn(move || {
            for index in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let body = read_http_request_body(&mut stream);
                captured
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap());
                let response_body = json!({
                    "id": format!("resp-unit-{index}"),
                    "model": "unit-profile-model",
                    "output": [{
                        "type": "message",
                        "role": "assistant",
                        "content": [{
                            "type": "output_text",
                            "text": answer_content
                        }]
                    }],
                    "usage": {
                        "input_tokens": 10,
                        "output_tokens": 5,
                        "total_tokens": 15,
                        "input_tokens_details": {"cached_tokens": 0},
                        "output_tokens_details": {
                            "reasoning_tokens": 0,
                            "accepted_prediction_tokens": 0,
                            "rejected_prediction_tokens": 0
                        }
                    }
                })
                .to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (endpoint, requests, handle)
    }

    fn start_response_capture_server_with_bodies(
        response_bodies: Vec<Value>,
    ) -> (
        String,
        Arc<std::sync::Mutex<Vec<Value>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            for response_body in response_bodies {
                let (mut stream, _) = listener.accept().unwrap();
                let body = read_http_request_body(&mut stream);
                captured
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&body).unwrap());
                let response_body = response_body.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
        });
        (endpoint, requests, handle)
    }

    fn read_http_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "HTTP request closed before headers completed");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some((header_end, content_length)) = http_body_bounds(&bytes) {
                let body_start = header_end + 4;
                while bytes.len() < body_start + content_length {
                    let read = stream.read(&mut buffer).unwrap();
                    assert_ne!(read, 0, "HTTP request closed before body completed");
                    bytes.extend_from_slice(&buffer[..read]);
                }
                return bytes[body_start..body_start + content_length].to_vec();
            }
        }
    }

    fn http_body_bounds(bytes: &[u8]) -> Option<(usize, usize)> {
        let header_end = bytes.windows(4).position(|window| window == b"\r\n\r\n")?;
        let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        Some((header_end, content_length))
    }

    fn prepare_memory_nokv(
        runs: &[CorpusRun],
        indexes: &DerivedIndexes,
    ) -> NoKvFs<HoltMetadataStore, nokv_object::MemoryObjectStore> {
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            nokv_object::MemoryObjectStore::new(),
        );
        service
            .bootstrap_root(DEFAULT_MODE_DIR, DEFAULT_UID, DEFAULT_GID)
            .unwrap();
        let repo = ArtifactRepository::with_options(
            DirectNoKvBackend { service: &service },
            ArtifactRepositoryOptions {
                producer: "unit-test".to_owned(),
                content_type: "application/octet-stream".to_owned(),
                ..ArtifactRepositoryOptions::default()
            },
        );
        for run in runs {
            repo.put_bytes(
                &format!("{NOKV_PREFIX}/runs/{}/metadata.json", run.id),
                run.metadata_bytes.clone(),
            )
            .unwrap();
            if let Some(bytes) = &run.params_bytes {
                repo.put_bytes(
                    &format!("{NOKV_PREFIX}/runs/{}/params.yaml", run.id),
                    bytes.clone(),
                )
                .unwrap();
            }
            if let Some(bytes) = &run.metrics_bytes {
                repo.put_bytes(
                    &format!("{NOKV_PREFIX}/runs/{}/metrics.json", run.id),
                    bytes.clone(),
                )
                .unwrap();
            }
            if let Some(bytes) = &run.dependencies_bytes {
                repo.put_bytes(
                    &format!("{NOKV_PREFIX}/runs/{}/dependencies.json", run.id),
                    bytes.clone(),
                )
                .unwrap();
            }
            for artifact in &run.artifacts {
                repo.put_bytes(
                    &format!(
                        "{NOKV_PREFIX}/runs/{}/artifacts/{}",
                        run.id, artifact.relative_path
                    ),
                    artifact.bytes.clone(),
                )
                .unwrap();
            }
        }
        for (path, bytes) in &indexes.files {
            repo.put_bytes(&format!("{NOKV_PREFIX}{}", path), bytes.clone())
                .unwrap();
        }
        register_nokv_agent_indexes(&service, runs).unwrap();
        service
    }

    #[test]
    fn run_summary_extracts_warm_interface_fields() {
        let summary = run_summary(&sample_run());
        assert_eq!(summary.experiment_id, "run-1");
        assert_eq!(summary.status.as_deref(), Some("completed"));
        assert_eq!(summary.script.as_deref(), Some("train.py"));
        assert_eq!(summary.tags, vec!["sweep", "final"]);
    }

    #[test]
    fn nokv_agent_index_row_uses_catalog_field_ids() {
        let mut run = sample_run();
        run.metadata["git"] = json!({
            "has_uncommitted_changes": true,
            "patch_file": "missing.patch"
        });
        let row = nokv_agent_index_row(&run);
        let values = row
            .values
            .iter()
            .map(|value| {
                (
                    value.field.id.as_str(),
                    match &value.value {
                        NamespacePredicateValue::String(value) => json!(value),
                        NamespacePredicateValue::U64(value) => json!(value),
                        NamespacePredicateValue::F64(value) => json!(value),
                        NamespacePredicateValue::List(value) => json!(value
                            .iter()
                            .map(|item| format!("{item:?}"))
                            .collect::<Vec<_>>()),
                    },
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(row.path, "/yanex/runs/run-1");
        assert!(values.contains(&("run.status", json!("completed"))));
        assert!(values.contains(&("run.script", json!("train.py"))));
        assert!(values.contains(&("run.tag", json!("sweep"))));
        assert!(values.contains(&("artifact.name", json!("stdout.txt"))));
        assert!(values.contains(&("artifact.stdout_available", json!(1))));
        assert!(values.contains(&("param.origami.training.learning_rate", json!("0.001"))));
        assert!(values.contains(&("param.origami.training.batch_size", json!("32"))));
        assert!(values.contains(&("metric.val_loss.min", json!(0.3))));
        assert!(values.contains(&("metric.utility_tstr_roc_auc.latest", json!(0.8))));
        assert!(values.contains(&("git.has_uncommitted_changes", json!(1))));
        assert!(values.contains(&("git.patch_available", json!(0))));
    }

    #[test]
    fn nokv_agent_index_fields_expose_params_metric_and_group_discovery_summaries() {
        let field_ids = nokv_agent_index_fields()
            .into_iter()
            .map(|field| field.field.id)
            .collect::<BTreeSet<_>>();

        assert!(field_ids.contains("param.origami.training.learning_rate"));
        assert!(field_ids.contains("param.origami.training.batch_size"));
        assert!(field_ids.contains("metric.val_loss.min"));
        assert!(field_ids.contains("metric.utility_tstr_roc_auc.latest"));
        assert!(field_ids.contains("git.has_uncommitted_changes"));
        assert!(field_ids.contains("group.lr_batch.representative"));
        assert!(field_ids.contains("group.lr_batch.run_count"));
        assert!(!field_ids.contains("group.lr_batch.avg_min_val_loss"));
        assert!(!field_ids.contains("group.lr_batch.avg_min_val_loss_scaled"));
    }

    #[test]
    fn train_lr_batch_groups_project_only_discovery_fields_into_run_index_rows() {
        let run = sample_run();
        let groups = train_lr_batch_groups(std::slice::from_ref(&run));
        let row = nokv_agent_index_row_with_groups(&run, &groups);
        let values = row
            .values
            .iter()
            .map(|value| {
                (
                    value.field.id.as_str(),
                    match &value.value {
                        NamespacePredicateValue::String(value) => json!(value),
                        NamespacePredicateValue::U64(value) => json!(value),
                        NamespacePredicateValue::F64(value) => json!(value),
                        NamespacePredicateValue::List(value) => json!(value
                            .iter()
                            .map(|item| format!("{item:?}"))
                            .collect::<Vec<_>>()),
                    },
                )
            })
            .collect::<Vec<_>>();

        assert!(values.contains(&("group.lr_batch.key", json!("lr=0.001;batch=32"))));
        assert!(values.contains(&("group.lr_batch.run_count", json!(1))));
        assert!(values.contains(&("group.lr_batch.representative", json!(1))));
        assert!(!values
            .iter()
            .any(|(field, _)| *field == "group.lr_batch.avg_min_val_loss"));
        assert!(!values
            .iter()
            .any(|(field, _)| *field == "group.lr_batch.avg_min_val_loss_scaled"));
    }

    #[test]
    fn file_body_oracle_returns_empty_rows_when_sample_data_has_no_cancelled_runs() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        insert_run(&conn, &sample_run()).unwrap();
        let task = find_task("cancelled_train_interrupt_triage").unwrap();

        let rows = query_gold_rows(&conn, &task).unwrap();

        assert!(rows.is_empty());
    }

    #[test]
    fn derived_indexes_use_profile_paths() {
        let indexes = derive_indexes(&[sample_run()]).unwrap();
        assert!(indexes.files.contains_key("/index/experiments.json"));
        assert!(indexes.files.contains_key("/index/status/completed.json"));
        assert!(indexes.files.contains_key("/index/tags/sweep.json"));
        assert!(indexes.files.contains_key("/index/scripts/train.py.json"));
    }

    #[test]
    fn sqlite_schema_materializes_raw_and_sqlite_file_views() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_agent_index_materialization(&conn, std::slice::from_ref(&run)).unwrap();
        insert_sqlite_run_files(&conn, &run).unwrap();
        for (path, bytes) in indexes.files {
            insert_file(&conn, &path, "file", &bytes).unwrap();
        }
        let status: String = conn
            .query_row(
                "SELECT status FROM experiments WHERE experiment_id='run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
        let file: Option<Vec<u8>> = conn
            .query_row(
                "SELECT content FROM files WHERE path='/runs/run-1/metadata.json'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(file.unwrap(), run.metadata_bytes);
        let metric: f64 = conn
            .query_row(
                "SELECT value FROM metrics WHERE experiment_id='run-1' AND metric_name='accuracy'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(metric, 0.9);
        let min_val_loss: f64 = conn
            .query_row(
                "SELECT metric_val_loss_min FROM run_agent_index WHERE experiment_id='run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(min_val_loss, 0.3);
        let schema = sqlite_schema_text(&conn).unwrap();
        assert!(schema.contains("CREATE TABLE experiments"));
        assert!(schema.contains("CREATE TABLE run_agent_index_catalog"));
        assert!(schema.contains("CREATE TABLE run_agent_index_values"));
        assert!(schema.contains("idx_run_agent_index_values_field_text"));
        assert!(!schema.contains("CREATE TABLE run_agent_index ("));
        assert!(!schema.contains("idx_run_agent_index_status_script"));
        assert!(!schema.contains("metric_val_loss_min"));
        assert!(!schema.contains("group_lr_batch_avg_min_val_loss"));
    }

    #[test]
    fn sqlite_agent_index_ensure_upgrades_existing_fixture_db() {
        let dir = tempfile::tempdir().unwrap();
        let sqlite_dir = dir.path().join("sqlite");
        fs::create_dir_all(&sqlite_dir).unwrap();
        let sqlite = sqlite_dir.join("yanex.db");
        let conn = Connection::open(&sqlite).unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();
        drop_agent_index_schema(&conn).unwrap();
        drop(conn);
        write_sample_corpus(dir.path(), &run);

        ensure_sqlite_agent_index_materialization(dir.path()).unwrap();

        let conn = Connection::open(&sqlite).unwrap();
        assert!(sqlite_agent_index_materialization_current(&conn).unwrap());
        let min_val_loss: f64 = conn
            .query_row(
                "SELECT metric_val_loss_min FROM run_agent_index WHERE experiment_id = 'run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(min_val_loss, 0.3);
    }

    #[test]
    fn sqlite_verifier_checks_all_materialized_surfaces() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_agent_index_materialization(&conn, std::slice::from_ref(&run)).unwrap();
        insert_sqlite_run_files(&conn, &run).unwrap();
        for (path, bytes) in indexes.files {
            insert_file(&conn, &path, "file", &bytes).unwrap();
        }
        insert_index_dirs(&conn, &derive_indexes(std::slice::from_ref(&run)).unwrap()).unwrap();

        let report = verify_sqlite_materialization(&conn).unwrap();

        assert_eq!(report.run_count, 1);
        assert_eq!(report.raw_metadata_checked, 1);
        assert_eq!(report.raw_params_checked, 4);
        assert_eq!(report.raw_metrics_checked, 4);
        assert_eq!(report.raw_dependencies_checked, 1);
        assert_eq!(report.raw_existing_artifacts_checked, 1);
        assert_eq!(report.sqlite_files_checked, 5);
        assert_eq!(report.index_files_checked, 5);
        assert_eq!(report.agent_index_rows_checked, 1);
        assert!(report.agent_index_values_checked > 0);
        assert_eq!(
            report.agent_index_catalog_checked,
            nokv_agent_index_fields().len()
        );
        assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    }

    #[test]
    fn nokv_verifier_treats_missing_artifact_parent_as_absent() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let mut run = sample_run();
        run.metadata["git"] = json!({
            "has_uncommitted_changes": true,
            "patch_file": "patches/missing.patch"
        });
        run.metadata_bytes = serde_json::to_vec(&run.metadata).unwrap();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        insert_sqlite_run_files(&conn, &run).unwrap();
        for (path, bytes) in &indexes.files {
            insert_file(&conn, path, "file", bytes).unwrap();
        }
        insert_index_dirs(&conn, &indexes).unwrap();
        let service = prepare_memory_nokv(std::slice::from_ref(&run), &indexes);

        let report = verify_nokv_namespace(&conn, &service).unwrap();

        assert_eq!(report.missing_artifacts_checked, 1);
        assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    }

    #[test]
    fn nokv_evidence_checker_treats_missing_parent_as_unsupported() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        let indexes = derive_indexes(std::slice::from_ref(&run)).unwrap();
        insert_run(&conn, &run).unwrap();
        let service = prepare_memory_nokv(std::slice::from_ref(&run), &indexes);

        let supported = nokv_native_evidence_supported(
            &conn,
            "nokv-native:///yanex/runs/run-1/artifacts/patches/missing.patch",
            Some(&service),
        )
        .unwrap();

        assert!(!supported);
    }

    #[test]
    fn sqlite_query_tool_wraps_rows_with_stable_evidence_refs() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();

        let result = execute_sqlite_query_tool(
            &conn,
            "SELECT experiment_id, artifact_path, blob_ref FROM artifacts ORDER BY artifact_path",
        )
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].evidence,
            vec![
                "sqlite://experiments/run-1".to_owned(),
                "sqlite://artifacts/run-1/stdout.txt".to_owned(),
                "sqlite://blobs/artifact:run-1:stdout.txt".to_owned(),
            ]
        );
    }

    #[test]
    fn sqlite_query_tool_timeout_interrupts_runaway_query() {
        let conn = Connection::open_in_memory().unwrap();
        let started = Instant::now();

        let err = execute_sqlite_query_tool_with_timeout(
            &conn,
            "WITH RECURSIVE cnt(x) AS (VALUES(0) UNION ALL SELECT x + 1 FROM cnt) SELECT count(*) FROM cnt",
            Duration::ZERO,
        )
        .unwrap_err();

        assert!(
            matches!(
                err,
                HarnessError::ToolTimeout {
                    ref tool,
                    limit_ms: 0
                } if tool == "query_sql"
            ),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn sqlite_read_blob_tool_returns_ranges_and_evidence() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();

        let result = read_blob_tool(&conn, "artifact:run-1:stdout.txt", 1, 2).unwrap();

        assert_eq!(result.blob_ref, "artifact:run-1:stdout.txt");
        assert_eq!(result.offset, 1);
        assert_eq!(result.bytes_read, 2);
        assert_eq!(result.content_utf8.as_deref(), Some("og"));
        assert_eq!(
            result.evidence,
            "sqlite://blobs/artifact:run-1:stdout.txt".to_owned()
        );
    }

    #[test]
    fn missing_git_patch_is_materialized_as_absent_artifact() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let mut run = sample_run();
        run.metadata["git"] = json!({
            "has_uncommitted_changes": true,
            "patch_file": "missing.patch"
        });
        insert_run(&conn, &run).unwrap();

        let exists: Option<i64> = conn
            .query_row(
                "SELECT \"exists\" FROM artifacts WHERE experiment_id='run-1' AND artifact_path='missing.patch'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        let blob_ref: Option<String> = conn
            .query_row(
                "SELECT diff_blob_ref FROM git_state WHERE experiment_id='run-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(exists, Some(0));
        assert_eq!(blob_ref, Some("artifact:run-1:missing.patch".to_owned()));
    }

    #[test]
    fn load_artifacts_skips_appledouble_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("stdout.txt"), b"log").unwrap();
        fs::write(dir.path().join("._stdout.txt"), b"appledouble").unwrap();

        let artifacts = load_artifacts(dir.path()).unwrap();

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].relative_path, "stdout.txt");
        assert_eq!(artifacts[0].bytes, b"log");
    }

    #[test]
    fn json_parse_error_includes_path() {
        let path = PathBuf::from("/tmp/metrics.json");
        let err = parse_json_bytes(&path, br#"{"value": NaN}"#).unwrap_err();

        assert!(err.to_string().contains("/tmp/metrics.json"));
        assert!(err.to_string().contains("expected value"));
    }

    #[test]
    fn metrics_parser_accepts_nonfinite_values() {
        let path = PathBuf::from("/tmp/metrics.json");
        let metrics = parse_metrics_bytes(
            &path,
            br#"[{"val_loss": Infinity, "other": -Infinity, "note": "Infinity"}]"#,
        )
        .unwrap();
        let record = metrics.as_array().unwrap()[0].as_object().unwrap();

        assert_eq!(metric_real(&record["val_loss"]), Some(f64::INFINITY));
        assert_eq!(metric_real(&record["other"]), Some(f64::NEG_INFINITY));
        assert_eq!(record["note"].as_str(), Some("Infinity"));
    }

    #[test]
    fn index_component_sanitizes_path_separators() {
        assert_eq!(safe_index_component("scripts/train.py"), "scripts_train.py");
    }

    #[test]
    fn tool_registry_exposes_only_the_selected_arm_surface() {
        assert_eq!(
            tool_names(&tool_registry_for_arm("sqlite_raw_v1").unwrap()),
            vec!["show_schema", "query_sql", "read_blob", "grep_blob"]
        );
        assert_eq!(
            tool_names(&tool_registry_for_arm("nokv_native_v1").unwrap()),
            vec!["ls", "stat", "catalog", "read", "aggregate", "find", "grep"]
        );
        assert!(tool_registry_for_arm("unknown_arm_v1").is_err());
    }

    #[test]
    fn nokv_native_grep_tool_maps_to_product_service_surface() {
        let objects = nokv_object::MemoryObjectStore::new();
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            objects,
        );
        service.bootstrap_root(0o755, 1000, 1000).unwrap();
        let runs = service.create_dir_path("/runs", 0o755, 1000, 1000).unwrap();
        service
            .publish_artifact(nokv_meta::PublishArtifact {
                parent: runs.attr.inode,
                name: nokv_types::DentryName::new(b"stdout.txt".to_vec()).unwrap(),
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                content_type: "text/plain".to_owned(),
                manifest_id: "runs/stdout.txt".to_owned(),
                bytes: b"first\nneedle hit\n".to_vec(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            })
            .unwrap();

        let result = execute_nokv_tool(
            &service,
            "grep",
            &json!({
                "path": "/runs",
                "pattern": "needle",
                "recursive": true,
                "limit": 10
            }),
        )
        .unwrap();

        assert_eq!(result["path"], "/runs");
        assert_eq!(result["matches"][0]["path"], "/runs/stdout.txt");
        assert_eq!(result["matches"][0]["line_number"], 2);
        assert_eq!(result["matches"][0]["snippet"], "needle hit");
        assert!(result.get("tool").is_none());
        assert!(result["matches"][0].get("evidence").is_none());
    }

    #[test]
    fn arm_cards_are_runtime_context_not_tool_contracts() {
        for (arm, card_text) in [
            (
                "nokv_native_v1",
                include_str!("../../agent-interface/arms/nokv_native.yaml"),
            ),
            (
                "sqlite_raw_v1",
                include_str!("../../agent-interface/arms/sqlite_raw.yaml"),
            ),
        ] {
            let card: serde_yaml::Value = serde_yaml::from_str(card_text).unwrap();
            assert!(
                card.get("runtime_context").is_some(),
                "{arm} card must expose runtime_context"
            );
            assert!(
                card.get("tool_contract").is_none(),
                "{arm} card must not duplicate tool registry contracts"
            );
            assert!(
                card.get("materialized_view_schema_hint").is_none(),
                "{arm} card must not duplicate SQL schema discovery guidance"
            );

            let lower = card_text.to_ascii_lowercase();
            for snippet in [
                "benchmark",
                "telemetry",
                "tool calls",
                "arguments:",
                "returns:",
                "find.match_count",
                "predicate_values",
            ] {
                assert!(
                    !lower.contains(snippet),
                    "{arm} card must not contain tool or benchmark contract snippet {snippet:?}"
                );
            }
        }
    }

    #[test]
    fn sql_tool_registry_documents_task_interface_semantics() {
        let tools = tool_registry_for_arm("sqlite_raw_v1").unwrap();
        let show_schema = tool_definition(&tools, "show_schema");
        let query_sql = tool_definition(&tools, "query_sql");
        let read_blob = tool_definition(&tools, "read_blob");

        assert!(show_schema.description.contains("live SQLite schema"));
        assert!(query_sql.description.contains("Only SELECT"));
        assert!(!query_sql.description.contains("evidence"));
        assert!(query_sql.parameters["properties"]["sql"]["description"]
            .as_str()
            .unwrap()
            .contains("read-only"));
        assert!(read_blob
            .description
            .contains("blob_ref returned by query_sql"));
        assert!(read_blob.description.contains("Use read_blob only"));
        assert!(
            read_blob.parameters["properties"]["blob_ref"]["description"]
                .as_str()
                .unwrap()
                .contains("query_sql")
        );
    }

    #[test]
    fn phase1_prompts_request_body_and_namespace_research_workflows() {
        let task_set = phase1_task_set().unwrap();
        let combined = task_set
            .tasks
            .iter()
            .map(|task| task.prompt.as_str())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(combined.contains("stdout log"));
        assert!(combined.contains("stderr log"));
        assert!(combined.contains("ddxplus_dcr"));
        assert!(combined.contains("KeyboardInterrupt"));
    }

    #[test]
    fn nokv_native_arm_card_does_not_embed_phase1_task_recipes() {
        let card = include_str!("../../agent-interface/arms/nokv_native.yaml");
        let forbidden_snippets = [
            "train.py",
            "eval.py",
            "lowest val_loss",
            "best eval",
            "grouped learning-rate/batch-size",
            "dirty git missing patch",
            "completed-only",
            "metric.val_loss.min_scaled",
            "metric.utility_tstr_roc_auc.latest_scaled",
            "group.lr_batch",
            "git.patch_available=0",
        ];

        for snippet in forbidden_snippets {
            assert!(
                !card.contains(snippet),
                "nokv_native arm card must not include phase1 task recipe snippet: {snippet}"
            );
        }
    }

    #[test]
    fn evidence_checker_supports_schema_and_validates_nokv_paths() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();
        insert_sqlite_run_files(&conn, &run).unwrap();

        assert!(
            evidence_handle_supported(&conn, "sqlite_raw_v1", "sqlite://schema", None).unwrap()
        );
        assert!(evidence_handle_supported(
            &conn,
            "nokv_native_v1",
            "nokv-native:///yanex/runs/run-1/metadata.json@generation:7",
            None,
        )
        .unwrap());
        assert!(!evidence_handle_supported(
            &conn,
            "nokv_native_v1",
            "nokv-native:///yanex/runs/missing/metadata.json@generation:7",
            None,
        )
        .unwrap());
        assert!(!evidence_handle_supported(
            &conn,
            "nokv_native_v1",
            "nokv-native:///yanex/runs/run-1/metadata.json@generation:not-a-number",
            None,
        )
        .unwrap());
    }

    #[test]
    fn phase1_tasks_are_researcher_shaped_prompts() {
        let task_set = phase1_task_set().unwrap();
        let task_ids: Vec<&str> = task_set
            .tasks
            .iter()
            .map(|task| task.task_id.as_str())
            .collect();

        assert_eq!(
            task_ids,
            vec![
                "train_top_configs_report",
                "eval_fidelity_leaderboard",
                "tabdiff_ddxplus_dcr_checkpoint_provenance",
                "best_detection_eval_method_audit",
                "cancelled_train_interrupt_triage",
            ]
        );
        assert!(!task_ids.contains(&"status_counts"));
        assert!(!task_ids.contains(&"train_lr_batch_loss_top5"));
        assert!(!task_ids.contains(&"eval_best_utility_tstr"));
        let oracle_task_ids = [
            "tabdiff_ddxplus_dcr_checkpoint_provenance",
            "best_detection_eval_method_audit",
            "cancelled_train_interrupt_triage",
        ];
        for task in &task_set.tasks {
            assert!(!task.prompt.trim().is_empty());
            if oracle_task_ids.contains(&task.task_id.as_str()) {
                assert!(task.gold_sql.is_none());
            } else {
                assert!(task.gold_sql.is_some());
            }
            let lower = task.prompt.to_ascii_lowercase();
            assert!(!lower.contains("watch"));
            assert!(!lower.contains("snapshot"));
            assert!(!lower.contains("overwrite"));
        }
    }

    #[test]
    fn readme_documents_one_core_valid_ab_comparison() {
        let readme = include_str!("../../agent-interface/README.md");

        assert!(readme.contains("## Valid Comparisons"));
        assert!(readme.contains("one core A/B comparison"));
        assert!(readme.contains("Raw SQLite tools vs NoKV Native Namespace"));
    }

    #[test]
    fn phase1_rubric_primary_metrics_match_summary_columns() {
        let rubric: serde_yaml::Value = serde_yaml::from_str(include_str!(
            "../../agent-interface/rubric/phase1_readonly.yaml"
        ))
        .unwrap();
        let primary_metrics = rubric
            .get("primary_metrics")
            .and_then(serde_yaml::Value::as_sequence)
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            primary_metrics,
            vec![
                "Arm",
                "Correctness",
                "Cached",
                "Uncached",
                "Completion",
                "tool_call_count",
                "wall_time_ms",
                "total_cost",
            ]
        );
    }

    #[test]
    fn base_profile_drives_runtime_request_defaults() {
        let profile = base_profile().unwrap();
        let options = Options::parse(&["--model".to_owned(), "unit-model".to_owned()]).unwrap();

        let config = BenchmarkRuntimeConfig::from_options(&options, &profile).unwrap();

        assert_eq!(config.model, "unit-model");
        assert_eq!(config.api_surface, ApiSurface::AgentsResponsesSchemaOnce);
        assert_eq!(config.max_turns, 20);
        assert_eq!(config.max_tool_calls, 80);
        assert_eq!(config.max_completion_tokens, 4096);
        assert_eq!(config.temperature, Some(1.0));
        assert_eq!(config.tool_call_timeout, Duration::from_millis(60_000));
        assert_eq!(config.response_format, Some(json!({"type": "json_object"})));
    }

    #[test]
    fn base_profile_does_not_configure_model_name() {
        let profile = base_profile().unwrap();

        assert!(!profile.yaml.contains("name_env:"));
        assert!(!profile.yaml.contains("OPENAI_MODEL"));
    }

    #[test]
    fn runtime_config_requires_explicit_model_not_env_fallback() {
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let _model = TestEnvVar::set("OPENAI_MODEL", "unit-env-model");
        let profile = base_profile().unwrap();
        let options = Options::default();

        let err = BenchmarkRuntimeConfig::from_options(&options, &profile).unwrap_err();

        assert_eq!(err.to_string(), "corpus error: --model is required");
    }

    #[test]
    fn cli_runtime_options_override_base_profile_defaults() {
        let profile = base_profile().unwrap();
        let options = Options::parse(&[
            "--api-surface".to_owned(),
            "openai_chat_completions".to_owned(),
            "--model".to_owned(),
            "unit-model".to_owned(),
            "--max-turns".to_owned(),
            "3".to_owned(),
            "--max-tool-calls".to_owned(),
            "4".to_owned(),
            "--max-completion-tokens".to_owned(),
            "2048".to_owned(),
            "--repeats".to_owned(),
            "2".to_owned(),
        ])
        .unwrap();

        let config = BenchmarkRuntimeConfig::from_options(&options, &profile).unwrap();

        assert_eq!(config.api_surface, ApiSurface::ChatCompletions);
        assert_eq!(config.max_turns, 3);
        assert_eq!(config.max_tool_calls, 4);
        assert_eq!(config.max_completion_tokens, 2048);
        assert_eq!(
            repeat_count_from_options(&options, &profile),
            2,
            "CLI repeat count should override base_profile.yaml"
        );
    }

    #[test]
    fn runtime_base_profile_path_overrides_default_profile_file() {
        let dir = tempfile::tempdir().unwrap();
        let profile_path = dir.path().join("base_profile.yaml");
        let profile_yaml =
            BASE_PROFILE_YAML.replace("max_completion_tokens: 4096", "max_completion_tokens: 1234");
        fs::write(&profile_path, &profile_yaml).unwrap();
        let options = Options::parse(&[
            "--base-profile".to_owned(),
            profile_path.display().to_string(),
            "--model".to_owned(),
            "unit-model".to_owned(),
        ])
        .unwrap();

        let profile = load_base_profile(&options).unwrap();
        let config = BenchmarkRuntimeConfig::from_options(&options, &profile).unwrap();

        assert_eq!(profile.yaml, profile_yaml);
        assert_eq!(config.max_completion_tokens, 1234);
    }

    #[test]
    fn base_profile_rejects_unsupported_stateful_runtime_policy() {
        let profile_yaml = BASE_PROFILE_YAML
            .replace("stateless: true", "stateless: false")
            .replace(
                "clear_messages_after_run: true",
                "clear_messages_after_run: false",
            );
        let profile = LoadedBaseProfile {
            profile: parse_base_profile_yaml(&profile_yaml).unwrap(),
            yaml: profile_yaml,
        };
        let options = Options::parse(&["--model".to_owned(), "unit-model".to_owned()]).unwrap();

        let err = BenchmarkRuntimeConfig::from_options(&options, &profile).unwrap_err();

        assert!(err
            .to_string()
            .contains("base_profile.run_policy.stateless=false is not supported"));
    }

    #[test]
    fn run_batch_applies_base_profile_and_restarts_messages_per_run() {
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("data");
        let output_jsonl = dir.path().join("telemetry.jsonl");
        prepare_sample_sqlite_data_root(&data_root);
        let profile_path = dir.path().join("base_profile.yaml");
        let profile_yaml = BASE_PROFILE_YAML
            .replace("temperature: 1", "temperature: 0.7")
            .replace("max_completion_tokens: 4096", "max_completion_tokens: 1234")
            .replace("repeats_per_arm_task: 10", "repeats_per_arm_task: 2")
            .replace("max_turns: 20", "max_turns: 1")
            .replace("max_tool_calls: 80", "max_tool_calls: 7")
            .replace(
                "You are an experiment tracking analysis agent working over a Yanex experiment corpus.",
                "Unit profile system marker.",
            )
            .replace(
                "Output exactly one JSON object matching the task's required answer shape.",
                "Unit profile developer marker.",
            );
        fs::write(&profile_path, &profile_yaml).unwrap();
        let (endpoint, requests, server) =
            start_response_capture_server(2, successful_empty_runs_answer());
        let _api_key = TestEnvVar::set("OPENAI_API_KEY", "unit-api-key");
        let _endpoint = TestEnvVar::set("OPENAI_RESPONSES_URL", &endpoint);
        let options = Options::parse(&[
            "--data-root".to_owned(),
            data_root.display().to_string(),
            "--output-jsonl".to_owned(),
            output_jsonl.display().to_string(),
            "--base-profile".to_owned(),
            profile_path.display().to_string(),
            "--api-surface".to_owned(),
            "openai_chat_completions".to_owned(),
            "--arm".to_owned(),
            "sqlite_raw_v1".to_owned(),
            "--task-id".to_owned(),
            "cancelled_train_interrupt_triage".to_owned(),
            "--model".to_owned(),
            "unit-profile-model".to_owned(),
        ])
        .unwrap();

        run_benchmark_batch(options).unwrap();
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "profile repeats_per_arm_task should drive run count when --repeats is omitted"
        );
        for request in requests.iter() {
            assert_eq!(request["model"], "unit-profile-model");
            assert_eq!(request["temperature"], 0.7);
            assert_eq!(request["max_output_tokens"], 1234);
            assert_eq!(request["text"]["format"], json!({"type": "json_object"}));
            assert!(request.get("previous_response_id").is_none());
            let tools = request["tools"].as_array().unwrap();
            assert!(tools.iter().any(|tool| tool["name"] == "query_sql"));
            assert!(tools.iter().all(|tool| tool.get("function").is_none()));
            let messages = request["input"].as_array().unwrap();
            assert_eq!(
                messages.len(),
                3,
                "each run should start from system/system/user context only"
            );
            assert_eq!(messages[0]["role"], "system");
            assert_eq!(messages[1]["role"], "system");
            assert_eq!(messages[2]["role"], "user");
            let combined_context = messages
                .iter()
                .filter_map(|message| message["content"].as_str())
                .collect::<Vec<_>>()
                .join("\n");
            let first_system = messages[0]["content"].as_str().unwrap();
            assert!(first_system.contains("Unit profile system marker."));
            let second_system = messages[1]["content"].as_str().unwrap();
            assert!(second_system.contains("Unit profile developer marker."));
            assert!(second_system.contains("sqlite_raw_v1"));
            assert!(second_system.contains("runtime_context"));
            assert!(!second_system.contains("query_sql"));
            assert!(messages[2]["content"]
                .as_str()
                .unwrap()
                .contains("Task ID: cancelled_train_interrupt_triage"));
            assert!(!combined_context.contains("Base profile YAML:"));
            assert!(!combined_context.contains("Rubric YAML:"));
            assert!(!combined_context.contains("gold_sql"));
            assert!(!combined_context.contains("SELECT status, COUNT(*) AS count"));
            assert!(!combined_context.contains("primary_metrics:"));
        }
        let telemetry = fs::read_to_string(output_jsonl).unwrap();
        assert_eq!(telemetry.lines().count(), 2);
        assert!(telemetry
            .lines()
            .all(|line| line.contains("\"task_success\":true")));
        assert!(telemetry
            .lines()
            .all(|line| line.contains("\"correctness\":true")));
    }

    #[test]
    fn run_task_omits_tool_schema_after_first_tool_turn() {
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("data");
        let output_jsonl = dir.path().join("telemetry.jsonl");
        prepare_sample_sqlite_data_root(&data_root);
        let profile_path = dir.path().join("base_profile.yaml");
        let profile_yaml = BASE_PROFILE_YAML
            .replace("max_turns: 20", "max_turns: 2")
            .replace("repeats_per_arm_task: 10", "repeats_per_arm_task: 1");
        fs::write(&profile_path, &profile_yaml).unwrap();
        let final_answer = successful_empty_runs_answer();
        let first_response = json!({
            "id": "resp-tool-0",
            "model": "unit-profile-model",
            "output": [{
                "type": "function_call",
                "id": "fc-query-status",
                "call_id": "call-query-status",
                "name": "query_sql",
                "arguments": r#"{"sql":"SELECT status, COUNT(*) AS count FROM experiments GROUP BY status ORDER BY status"}"#
            }],
            "usage": {
                "input_tokens": 100,
                "output_tokens": 10,
                "total_tokens": 110,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}
            }
        });
        let second_response = json!({
            "id": "resp-tool-1",
            "model": "unit-profile-model",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": final_answer.to_string()
                }]
            }],
            "usage": {
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}
            }
        });
        let (endpoint, requests, server) =
            start_response_capture_server_with_bodies(vec![first_response, second_response]);
        let _api_key = TestEnvVar::set("OPENAI_API_KEY", "unit-api-key");
        let _endpoint = TestEnvVar::set("OPENAI_RESPONSES_URL", &endpoint);
        let options = Options::parse(&[
            "--data-root".to_owned(),
            data_root.display().to_string(),
            "--output-jsonl".to_owned(),
            output_jsonl.display().to_string(),
            "--base-profile".to_owned(),
            profile_path.display().to_string(),
            "--api-surface".to_owned(),
            "openai_chat_completions".to_owned(),
            "--arm".to_owned(),
            "sqlite_raw_v1".to_owned(),
            "--task-id".to_owned(),
            "cancelled_train_interrupt_triage".to_owned(),
            "--model".to_owned(),
            "unit-profile-model".to_owned(),
        ])
        .unwrap();

        run_benchmark_batch(options).unwrap();
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].get("tools").is_some());
        assert!(requests[0].get("previous_response_id").is_none());
        assert!(
            requests[1].get("tools").is_none(),
            "tool schema must only be sent on the first model request in a task"
        );
        assert_eq!(requests[1]["previous_response_id"], "resp-tool-0");
        let tool_outputs = requests[1]["input"].as_array().unwrap();
        assert_eq!(tool_outputs.len(), 1);
        assert_eq!(tool_outputs[0]["type"], "function_call_output");
        assert_eq!(tool_outputs[0]["call_id"], "call-query-status");
        assert!(tool_outputs[0]["output"]
            .as_str()
            .unwrap()
            .contains("\"tool\":\"query_sql\""));
    }

    #[test]
    fn agent_sdk_runner_keeps_tools_available_after_first_tool_turn() {
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("data");
        let output_jsonl = dir.path().join("telemetry.jsonl");
        prepare_sample_sqlite_data_root(&data_root);
        let profile_path = dir.path().join("base_profile.yaml");
        let profile_yaml = BASE_PROFILE_YAML
            .replace("max_turns: 20", "max_turns: 2")
            .replace("repeats_per_arm_task: 10", "repeats_per_arm_task: 1");
        fs::write(&profile_path, &profile_yaml).unwrap();
        let final_answer = successful_empty_runs_answer();
        let first_response = json!({
            "id": "resp-agent-tool-0",
            "model": "unit-profile-model",
            "output": [{
                "type": "function_call",
                "id": "fc-query-status",
                "call_id": "call-query-status",
                "name": "query_sql",
                "arguments": r#"{"sql":"SELECT status, COUNT(*) AS count FROM experiments GROUP BY status ORDER BY status"}"#
            }],
            "usage": {
                "input_tokens": 100,
                "output_tokens": 10,
                "total_tokens": 110,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}
            }
        });
        let second_response = json!({
            "id": "resp-agent-tool-1",
            "model": "unit-profile-model",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": final_answer.to_string()
                }]
            }],
            "usage": {
                "input_tokens": 50,
                "output_tokens": 10,
                "total_tokens": 60,
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens_details": {"reasoning_tokens": 0}
            }
        });
        let (endpoint, requests, server) =
            start_response_capture_server_with_bodies(vec![first_response, second_response]);
        let _api_key = TestEnvVar::set("OPENAI_API_KEY", "unit-api-key");
        let _endpoint = TestEnvVar::set("OPENAI_RESPONSES_URL", &endpoint);
        let _no_sdk = TestEnvVar::set("YANEX_BENCH_AGENT_SDK_TEST_NO_SDK", "1");
        let _allow_http = TestEnvVar::set("YANEX_BENCH_ALLOW_INSECURE_OPENAI_ENDPOINT", "1");
        let options = Options::parse(&[
            "--data-root".to_owned(),
            data_root.display().to_string(),
            "--output-jsonl".to_owned(),
            output_jsonl.display().to_string(),
            "--base-profile".to_owned(),
            profile_path.display().to_string(),
            "--arm".to_owned(),
            "sqlite_raw_v1".to_owned(),
            "--task-id".to_owned(),
            "cancelled_train_interrupt_triage".to_owned(),
            "--model".to_owned(),
            "unit-profile-model".to_owned(),
        ])
        .unwrap();

        run_benchmark_batch(options).unwrap();
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].get("tools").is_some());
        assert!(requests[0].get("previous_response_id").is_none());
        assert_eq!(
            requests[0]["text"]["format"],
            json!({"type": "json_object"})
        );
        let continuation_tools = requests[1]["tools"].as_array().unwrap();
        assert!(
            continuation_tools
                .iter()
                .any(|tool| tool["name"] == "query_sql"),
            "Agent SDK continuation must keep tools available for additional calls"
        );
        assert!(
            requests[1].get("text").is_none(),
            "Agent SDK schema-once runner must not repeat initial structured-output config"
        );
        assert_eq!(
            requests[1]["model"], "unit-profile-model",
            "Agent SDK schema-once continuation must include the Responses API required model"
        );
        assert!(
            requests[1].get("max_output_tokens").is_none(),
            "Agent SDK continuation must not repeat max_output_tokens"
        );
        assert!(
            requests[1].get("temperature").is_none(),
            "Agent SDK continuation must not repeat temperature"
        );
        assert_eq!(requests[1]["previous_response_id"], "resp-agent-tool-0");
        let tool_outputs = requests[1]["input"].as_array().unwrap();
        assert_eq!(tool_outputs.len(), 1);
        assert_eq!(tool_outputs[0]["type"], "function_call_output");
        assert_eq!(tool_outputs[0]["call_id"], "call-query-status");
        assert!(tool_outputs[0]["output"]
            .as_str()
            .unwrap()
            .contains("\"tool\":\"query_sql\""));

        let benchmark_line = fs::read_to_string(output_jsonl)
            .unwrap()
            .lines()
            .find(|line| line.contains("\"record_type\":\"benchmark_run\""))
            .unwrap()
            .to_owned();
        let telemetry: Value = serde_json::from_str(&benchmark_line).unwrap();
        assert_eq!(
            telemetry["api_calls"][0]["response_id"],
            "resp-agent-tool-0"
        );
        assert_eq!(telemetry["api_calls"][0]["sent_tool_schema"], true);
        assert_eq!(telemetry["api_calls"][0]["sent_initial_instructions"], true);
        assert_eq!(
            telemetry["api_calls"][1]["previous_response_id"],
            "resp-agent-tool-0"
        );
        assert_eq!(telemetry["api_calls"][1]["sent_tool_schema"], true);
        assert_eq!(
            telemetry["api_calls"][1]["sent_initial_instructions"],
            false
        );
        assert_eq!(
            telemetry["derived_metrics"]["tool_schema_repeated_count"],
            1
        );
        assert_eq!(telemetry["derived_metrics"]["tool_call_count"], 1);
    }

    #[test]
    fn agent_sdk_runner_starts_each_repeat_without_previous_response_id() {
        let _env_lock = ENV_MUTEX.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("data");
        let output_jsonl = dir.path().join("telemetry.jsonl");
        prepare_sample_sqlite_data_root(&data_root);
        let profile_path = dir.path().join("base_profile.yaml");
        let profile_yaml = BASE_PROFILE_YAML
            .replace("max_turns: 20", "max_turns: 1")
            .replace("repeats_per_arm_task: 10", "repeats_per_arm_task: 2");
        fs::write(&profile_path, &profile_yaml).unwrap();
        let (endpoint, requests, server) =
            start_response_capture_server(2, successful_empty_runs_answer());
        let _api_key = TestEnvVar::set("OPENAI_API_KEY", "unit-api-key");
        let _endpoint = TestEnvVar::set("OPENAI_RESPONSES_URL", &endpoint);
        let _no_sdk = TestEnvVar::set("YANEX_BENCH_AGENT_SDK_TEST_NO_SDK", "1");
        let _allow_http = TestEnvVar::set("YANEX_BENCH_ALLOW_INSECURE_OPENAI_ENDPOINT", "1");
        let options = Options::parse(&[
            "--data-root".to_owned(),
            data_root.display().to_string(),
            "--output-jsonl".to_owned(),
            output_jsonl.display().to_string(),
            "--base-profile".to_owned(),
            profile_path.display().to_string(),
            "--arm".to_owned(),
            "sqlite_raw_v1".to_owned(),
            "--task-id".to_owned(),
            "cancelled_train_interrupt_triage".to_owned(),
            "--model".to_owned(),
            "unit-profile-model".to_owned(),
        ])
        .unwrap();

        run_benchmark_batch(options).unwrap();
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert!(request.get("previous_response_id").is_none());
            assert!(request.get("tools").is_some());
            assert!(request["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["role"] == "system"));
        }
        let telemetry = fs::read_to_string(output_jsonl).unwrap();
        let runs = telemetry
            .lines()
            .filter(|line| line.contains("\"record_type\":\"benchmark_run\""))
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(runs.len(), 2);
        for run in runs {
            assert_eq!(run["api_calls"][0]["previous_response_id"], Value::Null);
            assert_eq!(run["api_calls"][0]["sent_tool_schema"], true);
            assert_eq!(run["api_calls"][0]["sent_initial_instructions"], true);
        }
    }

    #[test]
    fn agent_visible_context_only_requests_task_answer() {
        let profile = base_profile().unwrap();
        let task = find_task("cancelled_train_interrupt_triage").unwrap();
        let messages = benchmark_messages(&profile, arm_card_yaml("sqlite_raw_v1").unwrap(), &task);
        let combined_context = messages
            .iter()
            .filter_map(|message| message.content.as_deref())
            .collect::<Vec<_>>()
            .join("\n")
            .to_ascii_lowercase();

        for forbidden in [
            "evidence",
            "missing_evidence",
            "operations_summary",
            "confidence",
            "run_ids",
            "benchmark schema",
        ] {
            assert!(
                !combined_context.contains(forbidden),
                "agent-visible context must not contain {forbidden:?}"
            );
        }
    }

    #[test]
    fn phase1_task_prompts_do_not_request_agent_visible_audit_fields() {
        let task_set = phase1_task_set().unwrap();

        for task in &task_set.tasks {
            let prompt = task.prompt.to_ascii_lowercase();
            for forbidden in ["cite evidence", "missing_evidence", "run_ids"] {
                assert!(
                    !prompt.contains(forbidden),
                    "{} prompt must not request {forbidden:?}",
                    task.task_id
                );
            }
        }
    }

    #[test]
    fn batch_plan_defaults_to_two_arms_five_tasks_ten_repeats() {
        let profile = base_profile().unwrap();
        let tasks = phase1_task_set().unwrap();
        let repeat_count = repeat_count_from_options(&Options::default(), &profile);
        let plan = batch_plan(None, None, repeat_count, &tasks);

        assert_eq!(repeat_count, 10);
        assert_eq!(plan.len(), 2 * 5 * 10);
        assert_eq!(benchmark_arm_ids(), &["sqlite_raw_v1", "nokv_native_v1"]);
        assert_eq!(plan[0].arm_id, "sqlite_raw_v1");
        assert_eq!(plan[0].task_id, "train_top_configs_report");
        assert_eq!(plan[0].repeat_index, 0);
        assert_eq!(plan[9].repeat_index, 9);
        assert_eq!(plan[10].task_id, "eval_fidelity_leaderboard");
    }

    #[test]
    fn batch_plan_repeats_every_task_for_selected_arm() {
        let tasks = phase1_task_set().unwrap();
        let plan = batch_plan(Some("nokv_native_v1"), None, 5, &tasks);

        assert_eq!(plan.len(), 5 * 5);
        assert_eq!(plan[0].task_id, "train_top_configs_report");
        assert_eq!(plan[0].repeat_index, 0);
        assert_eq!(plan[4].task_id, "train_top_configs_report");
        assert_eq!(plan[4].repeat_index, 4);
        assert_eq!(plan[5].task_id, "eval_fidelity_leaderboard");
        assert_eq!(plan[5].repeat_index, 0);
        assert_eq!(plan[24].task_id, "cancelled_train_interrupt_triage");
        assert_eq!(plan[24].repeat_index, 4);
    }

    #[test]
    fn nokv_native_tool_registry_exposes_body_grep() {
        let tools = tool_registry_for_arm("nokv_native_v1").unwrap();
        let tool_names = tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<BTreeSet<_>>();

        assert!(tool_names.contains("grep"));
        assert!(!tool_names.contains("research_audit"));
    }

    #[test]
    fn grep_blob_tool_returns_case_insensitive_line_matches() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let mut run = sample_run();
        run.artifacts = vec![CorpusFile {
            relative_path: "stderr.txt".to_owned(),
            bytes: b"Traceback\n  File \"train.py\", line 10\nKeyboardInterrupt\n".to_vec(),
        }];
        insert_run(&conn, &run).unwrap();
        let blob_ref: String = conn
            .query_row(
                "SELECT blob_ref FROM artifacts WHERE artifact_path = 'stderr.txt'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let result = execute_sqlite_raw_tool(
            &conn,
            "grep_blob",
            &json!({"blob_ref": blob_ref, "pattern": "keyboardinterrupt"}),
        )
        .unwrap();

        let matches = result["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["line_number"], 3);
        assert_eq!(matches[0]["snippet"], "KeyboardInterrupt");
        assert_eq!(result["truncated"], false);
        let evidence = matches[0]["evidence"].as_str().unwrap().to_owned();
        assert!(evidence.starts_with("sqlite://blobs/"));
        assert!(evidence.ends_with("#L3"));
        assert!(evidence_handle_supported(&conn, "sqlite_raw_v1", &evidence, None).unwrap());
    }

    #[test]
    fn local_judge_can_use_file_body_oracle_without_gold_sql() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let mut run = sample_run();
        run.id = "cancel-1".to_owned();
        run.metadata["id"] = json!("cancel-1");
        run.metadata["status"] = json!("cancelled");
        run.metadata["cli_args"]["script"] = json!("scripts/train.py");
        run.metadata_bytes = serde_json::to_vec(&run.metadata).unwrap();
        run.artifacts = vec![CorpusFile {
            relative_path: "stderr.txt".to_owned(),
            bytes:
                b"Traceback\n  File \"/tmp/train.py\", line 10, in <module>\nKeyboardInterrupt\n"
                    .to_vec(),
        }];
        insert_run(&conn, &run).unwrap();
        let task = BenchmarkTask {
            task_id: "cancelled_train_interrupt_triage".to_owned(),
            category: "failure_triage".to_owned(),
            prompt: "return cancelled keyboard interrupts".to_owned(),
            gold_sql: None,
            expected: ExpectedSpec {
                kind: "rows_exact".to_owned(),
                answer_key: Some("runs".to_owned()),
                run_id_column: Some("experiment_id".to_owned()),
                columns: vec![
                    "experiment_id".to_owned(),
                    "status".to_owned(),
                    "stderr_size_bytes".to_owned(),
                    "keyboard_interrupt".to_owned(),
                    "interrupt_line_number".to_owned(),
                ],
            },
        };
        let answer = json!({
            "runs": [{
                "experiment_id": "cancel-1",
                "status": "cancelled",
                "stderr_size_bytes": 73,
                "keyboard_interrupt": true,
                "interrupt_line_number": 3
            }]
        });

        let result = judge_answer(&conn, &task, "sqlite_raw_v1", &answer, None).unwrap();

        assert!(result.task_success, "{result:?}");
        assert_eq!(result.expected_run_ids, vec!["cancel-1"]);
    }

    #[test]
    fn repeat_option_alias_sets_repeat_count_not_task_count() {
        let options = Options::parse(&["--repeat".to_owned(), "5".to_owned()]).unwrap();

        assert_eq!(options.repeats, Some(5));
    }

    #[test]
    fn local_judge_checks_gold_rows_and_derives_run_ids_from_answer() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();
        let task = BenchmarkTask {
            task_id: "unit".to_owned(),
            category: "structured_analytics".to_owned(),
            prompt: "return the run".to_owned(),
            gold_sql: Some(
                "SELECT experiment_id, status FROM experiments ORDER BY experiment_id".to_owned(),
            ),
            expected: ExpectedSpec {
                kind: "rows_exact".to_owned(),
                answer_key: Some("runs".to_owned()),
                run_id_column: Some("experiment_id".to_owned()),
                columns: vec!["experiment_id".to_owned(), "status".to_owned()],
            },
        };
        let answer = json!({
            "runs": [{"experiment_id": "run-1", "status": "completed"}]
        });

        let result = judge_answer(&conn, &task, "sqlite_raw_v1", &answer, None).unwrap();

        assert!(result.task_success, "{result:?}");
        assert_eq!(result.evidence_precision, None);
        assert_eq!(result.expected_run_ids, vec!["run-1"]);
        assert_eq!(result.actual_run_ids, vec!["run-1"]);
    }

    #[test]
    fn local_judge_accepts_pure_task_answer_without_agent_audit_fields() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        let run = sample_run();
        insert_run(&conn, &run).unwrap();
        let task = BenchmarkTask {
            task_id: "unit".to_owned(),
            category: "structured_analytics".to_owned(),
            prompt: "return the run".to_owned(),
            gold_sql: Some(
                "SELECT experiment_id, status FROM experiments ORDER BY experiment_id".to_owned(),
            ),
            expected: ExpectedSpec {
                kind: "rows_exact".to_owned(),
                answer_key: Some("runs".to_owned()),
                run_id_column: Some("experiment_id".to_owned()),
                columns: vec!["experiment_id".to_owned(), "status".to_owned()],
            },
        };
        let answer = json!({
            "runs": [{"experiment_id": "run-1", "status": "completed"}]
        });

        let result = judge_answer(&conn, &task, "sqlite_raw_v1", &answer, None).unwrap();

        assert!(result.task_success, "{result:?}");
        assert_eq!(result.evidence_precision, None);
        assert_eq!(result.evidence_checked, 0);
        assert_eq!(result.evidence_supported, 0);
        assert_eq!(result.expected_run_ids, vec!["run-1"]);
        assert_eq!(result.actual_run_ids, vec!["run-1"]);
    }

    #[test]
    fn telemetry_records_cost_correctness_and_tool_metrics_as_jsonl() {
        let telemetry = BenchmarkRunTelemetry::sample_for_test();
        let line = telemetry.to_jsonl_line();
        let parsed: Value = serde_json::from_str(&line).unwrap();

        assert_eq!(parsed["record_type"], "benchmark_run");
        assert_eq!(parsed["run_error"], Value::Null);
        assert_eq!(parsed["api_calls"][0]["request_id"], "req-test");
        assert_eq!(parsed["api_calls"][0]["response_id"], "resp-test");
        assert_eq!(parsed["api_calls"][0]["previous_response_id"], Value::Null);
        assert_eq!(parsed["api_calls"][0]["sent_tool_schema"], true);
        assert_eq!(parsed["api_calls"][0]["sent_initial_instructions"], true);
        assert_eq!(parsed["derived_metrics"]["tool_call_count"], 1);
        assert_eq!(parsed["derived_metrics"]["tool_schema_repeated_count"], 0);
        assert_eq!(parsed["derived_metrics"]["invalid_sql_count"], 0);
        assert_eq!(parsed["derived_metrics"]["wrong_tool_count"], 0);
        assert_eq!(parsed["derived_metrics"]["tool_timeout_count"], 0);
        assert_eq!(parsed["derived_metrics"]["missing_evidence_count"], 0);
        assert_eq!(parsed["derived_metrics"]["overread_bytes"], 0);
        assert_eq!(parsed["correctness"], true);
        assert_eq!(parsed["judge"]["task_success"], true);
    }

    #[test]
    fn telemetry_counts_repeated_tool_schema_only_after_continuation() {
        let mut calls = BenchmarkRunTelemetry::sample_for_test().api_calls;
        calls.push(ApiCallTelemetry {
            request_id: Some("req-repeat".to_owned()),
            response_id: Some("resp-repeat".to_owned()),
            model: "test-model".to_owned(),
            started_at_unix_ms: 3,
            completed_at_unix_ms: 4,
            previous_response_id: Some("resp-test".to_owned()),
            sent_tool_schema: true,
            sent_initial_instructions: false,
            prompt_tokens: Some(1),
            completion_tokens: Some(1),
            total_tokens: Some(2),
            reasoning_tokens: Some(0),
            cached_prompt_tokens: Some(0),
            accepted_prediction_tokens: Some(0),
            rejected_prediction_tokens: Some(0),
        });

        assert_eq!(tool_schema_repeated_count(&calls), 1);
    }

    #[test]
    fn tool_call_start_record_serializes_before_completion() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl = dir.path().join("telemetry.jsonl");
        let call = OpenAiToolCall {
            id: "call-test".to_owned(),
            tool_type: "function".to_owned(),
            function: OpenAiToolCallFunction {
                name: "query_sql".to_owned(),
                arguments: r#"{"sql":"SELECT 1"}"#.to_owned(),
            },
        };

        append_tool_call_start_jsonl(
            &jsonl,
            "run-test",
            "sqlite_raw_v1",
            "status_counts",
            2,
            &call,
            123,
        )
        .unwrap();

        let text = fs::read_to_string(jsonl).unwrap();
        let parsed: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(parsed["record_type"], "tool_call_start");
        assert_eq!(parsed["run_id"], "run-test");
        assert_eq!(parsed["tool_name"], "query_sql");
        assert_eq!(parsed["arguments"]["sql"], "SELECT 1");
    }

    #[test]
    fn tool_bridge_server_invokes_sqlite_tool_and_records_telemetry() {
        let dir = tempfile::tempdir().unwrap();
        let data_root = dir.path().join("data");
        let output_jsonl = dir.path().join("telemetry.jsonl");
        prepare_sample_sqlite_data_root(&data_root);
        let tools = tool_registry_for_arm("sqlite_raw_v1").unwrap();
        let bridge = ToolBridgeServer::start(ToolBridgeStartConfig {
            run_id: "run-bridge",
            arm: "sqlite_raw_v1",
            task_id: "status_counts",
            repeat_index: 0,
            data_root: &data_root,
            s3: &S3Options::default(),
            registry: &tools,
            timeout: Duration::from_millis(5_000),
            output_jsonl: output_jsonl.clone(),
        })
        .unwrap();

        let response: Value = reqwest::blocking::Client::new()
            .post(format!("{}/invoke", bridge.url()))
            .json(&json!({
                "run_id": "run-bridge",
                "tool_name": "query_sql",
                "arguments": {
                    "sql": "SELECT status, COUNT(*) AS count FROM experiments GROUP BY status ORDER BY status"
                }
            }))
            .send()
            .unwrap()
            .json()
            .unwrap();
        let conn = Connection::open_with_flags(
            sqlite_path(&data_root),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let direct = execute_sqlite_raw_tool(
            &conn,
            "query_sql",
            &json!({
                "sql": "SELECT status, COUNT(*) AS count FROM experiments GROUP BY status ORDER BY status"
            }),
        )
        .unwrap();

        assert_eq!(response["status"], "success");
        assert_eq!(response["content"], direct);
        let snapshot = bridge.finish().unwrap();
        assert_eq!(snapshot.tool_calls.len(), 1);
        assert_eq!(snapshot.tool_calls[0].tool_name, "query_sql");
        assert_eq!(snapshot.tool_calls[0].bytes_read, 0);
        assert_eq!(snapshot.tool_call_count(), 1);

        let telemetry = fs::read_to_string(output_jsonl).unwrap();
        let first_line: Value = serde_json::from_str(telemetry.lines().next().unwrap()).unwrap();
        assert_eq!(first_line["record_type"], "tool_call_start");
        assert_eq!(first_line["run_id"], "run-bridge");
        assert_eq!(first_line["tool_name"], "query_sql");
    }

    #[test]
    fn only_context_length_http_errors_are_recoverable_run_errors() {
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "OpenAI response HTTP 400 Bad Request: context_length_exceeded".to_owned()
        )));
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "Input tokens exceed the configured limit of 272000 tokens".to_owned()
        )));
        assert!(is_recoverable_run_api_error(&HarnessError::Http(
            "model did not produce a final JSON answer before max_turns".to_owned()
        )));
        assert!(!is_recoverable_run_api_error(&HarnessError::Http(
            "OpenAI response HTTP 500 Internal Server Error".to_owned()
        )));
    }

    #[test]
    fn reqwest_send_error_diagnostic_includes_classification_and_sources() {
        let err = reqwest::blocking::Client::new()
            .post("not a url")
            .send()
            .unwrap_err();

        let diagnostic = model_response_send_error_diagnostic(2, 3, &err);

        assert!(diagnostic.contains("attempt=2/3"));
        assert!(diagnostic.contains("is_timeout="));
        assert!(diagnostic.contains("is_connect="));
        assert!(diagnostic.contains("is_request="));
        assert!(diagnostic.contains("error=builder error"));
        assert!(diagnostic.contains("source[0]="));
    }

    #[test]
    fn response_retry_budget_is_ten_attempts() {
        assert_eq!(RESPONSE_MAX_ATTEMPTS, 10);
    }

    #[test]
    fn judge_shape_errors_are_recoverable_run_errors() {
        assert!(is_recoverable_run_judge_error(&HarnessError::Judge(
            "answer.groups must be an array".to_owned()
        )));
        assert!(!is_recoverable_run_judge_error(&HarnessError::NoKv(
            "metadata store failed".to_owned()
        )));
    }

    fn tool_names(tools: &[ToolDefinition]) -> Vec<&str> {
        tools.iter().map(|tool| tool.name.as_str()).collect()
    }

    fn tool_definition<'a>(tools: &'a [ToolDefinition], name: &str) -> &'a ToolDefinition {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("{name} tool must be registered"))
    }
}
