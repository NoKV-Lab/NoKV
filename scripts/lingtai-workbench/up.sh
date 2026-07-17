#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="${ROOT_DIR}/scripts/lingtai-workbench"
STATE_DIR="${ROOT_DIR}/target/lingtai-workbench"
CANDIDATE_NOKV_BIN="${NOKV_BIN:-}"
NOKV_BIN=""
SERVER_BIND="${LINGTAI_WORKBENCH_SERVER_BIND:-127.0.0.1:7799}"
S3_ENDPOINT="${LINGTAI_WORKBENCH_S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_BUCKET="${LINGTAI_WORKBENCH_S3_BUCKET:-nokv-lingtai-workbench}"
OBJECT_BACKEND="${LINGTAI_WORKBENCH_OBJECT_BACKEND:-rustfs}"
WORKBENCH_ROOT="${LINGTAI_WORKBENCH_ROOT:-/agents/{agent_id}/wb}"
META_DIR="${LINGTAI_WORKBENCH_META_DIR:-${STATE_DIR}/meta}"
SERVER_LOG="${LINGTAI_WORKBENCH_SERVER_LOG:-${STATE_DIR}/nokv-server.log}"
SERVER_PID="${LINGTAI_WORKBENCH_SERVER_PID:-${STATE_DIR}/nokv-server.pid}"
SERVER_STATE="${LINGTAI_WORKBENCH_SERVER_STATE:-${STATE_DIR}/nokv-server.json}"
TUI_PYTHON="${LINGTAI_TUI_PYTHON:-${HOME}/.lingtai-tui/runtime/venv/bin/python}"
RUNTIME_IDENTITY_ARGS=()
SERVER_ARGV=()
UP_LOCK_DIR="${STATE_DIR}/up.lock"

log() {
  printf '[lingtai-workbench] %s\n' "$*"
}

die() {
  printf '[lingtai-workbench] error: %s\n' "$*" >&2
  exit 1
}

resolve_project() {
  if [[ -n "${LINGTAI_WORKBENCH_PROJECT:-}" ]]; then
    printf '%s\n' "${LINGTAI_WORKBENCH_PROJECT}"
    return
  fi
  if [[ -d ".lingtai" ]]; then
    pwd
    return
  fi
  if [[ -d "${HOME}/lingtai-demo/.lingtai" ]]; then
    printf '%s\n' "${HOME}/lingtai-demo"
    return
  fi
  die "cannot find a LingTai project; set LINGTAI_WORKBENCH_PROJECT=/path/to/project"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

release_up_lock() {
  if [[ -f "${UP_LOCK_DIR}/pid" ]] && [[ "$(cat "${UP_LOCK_DIR}/pid")" == "$$" ]]; then
    rm -f "${UP_LOCK_DIR}/pid"
    rmdir "${UP_LOCK_DIR}" 2>/dev/null || true
  fi
}

abort_up() {
  local status="$1"
  trap - INT TERM
  exit "${status}"
}

acquire_up_lock() {
  mkdir -p "${STATE_DIR}"
  if ! mkdir "${UP_LOCK_DIR}" 2>/dev/null; then
    local owner=""
    if [[ -f "${UP_LOCK_DIR}/pid" ]]; then
      owner="$(cat "${UP_LOCK_DIR}/pid")"
    fi
    if [[ "${owner}" =~ ^[0-9]+$ ]] && kill -0 "${owner}" >/dev/null 2>&1; then
      die "another lingtai-workbench up.sh is active (pid ${owner})"
    fi
    rm -f "${UP_LOCK_DIR}/pid"
    rmdir "${UP_LOCK_DIR}" 2>/dev/null ||
      die "stale update lock requires manual inspection: ${UP_LOCK_DIR}"
    mkdir "${UP_LOCK_DIR}"
  fi
  printf '%s\n' "$$" >"${UP_LOCK_DIR}/pid"
  trap release_up_lock EXIT
  trap 'abort_up 130' INT
  trap 'abort_up 143' TERM
}

prepare_runtime() {
  local project="$1"
  local candidate distribution
  local stage_args=(--stage-only --project "${project}")

  if [[ -z "${CANDIDATE_NOKV_BIN}" ]]; then
    require_cmd cargo
    require_cmd git
    distribution="source"
    stage_args+=(--build-source "${ROOT_DIR}" --distribution source)
    if [[ "${LINGTAI_WORKBENCH_ALLOW_DIRTY:-0}" == "1" ]]; then
      stage_args+=(--allow-dirty)
    fi
  else
    candidate="${CANDIDATE_NOKV_BIN}"
    [[ -n "${NOKV_BUILD_INFO:-}" ]] ||
      die "external NOKV_BIN requires its artifact-bound NOKV_BUILD_INFO"
    stage_args+=(--nokv-bin "${candidate}" --build-info "${NOKV_BUILD_INFO}")
    if [[ -n "${NOKV_REVISION:-}" ]]; then
      stage_args+=(--revision "${NOKV_REVISION}")
    fi
    if [[ -n "${NOKV_DISTRIBUTION:-}" ]]; then
      distribution="${NOKV_DISTRIBUTION}"
    elif [[ "${candidate}" == *"/Cellar/"* || "${candidate}" == *"/Homebrew/"* ]]; then
      distribution="brew"
    else
      distribution="release"
    fi
    stage_args+=(--distribution "${distribution}")
    if [[ "${LINGTAI_WORKBENCH_ALLOW_DIRTY:-0}" == "1" ]]; then
      stage_args+=(--allow-dirty)
    fi
  fi

  if [[ -n "${NOKV_EXPECTED_SHA256:-}" ]]; then
    stage_args+=(--expected-sha256 "${NOKV_EXPECTED_SHA256}")
  fi
  NOKV_BIN="$(python3 "${SCRIPT_DIR}/sync_workbench_mcp.py" "${stage_args[@]}")"
  [[ -x "${NOKV_BIN}" ]] || die "staged NoKV runtime is not executable: ${NOKV_BIN}"
  RUNTIME_IDENTITY_ARGS=(
    --build-info "$(dirname "${NOKV_BIN}")/build-info.json"
    --distribution "${distribution}"
  )
  if [[ "${LINGTAI_WORKBENCH_ALLOW_DIRTY:-0}" == "1" ]]; then
    RUNTIME_IDENTITY_ARGS+=(--allow-dirty)
  fi
  SERVER_ARGV=(
    "${NOKV_BIN}"
    --server-bind "${SERVER_BIND}"
    --object-backend "${OBJECT_BACKEND}"
    --s3-endpoint "${S3_ENDPOINT}"
    --s3-bucket "${S3_BUCKET}"
    --meta "${META_DIR}"
    serve
  )
  log "immutable NoKV runtime: ${NOKV_BIN}"
}

probe_candidate_contract() {
  local project="$1"
  local args=(
    --probe-only
    --project "${project}"
    --nokv-bin "${NOKV_BIN}"
    "${RUNTIME_IDENTITY_ARGS[@]}"
    --server-bind "${SERVER_BIND}"
    --object-backend "${OBJECT_BACKEND}"
    --s3-endpoint "${S3_ENDPOINT}"
    --s3-bucket "${S3_BUCKET}"
    --workbench-root "${WORKBENCH_ROOT}"
  )
  if [[ -n "${LINGTAI_WORKBENCH_AGENT:-}" ]]; then
    args+=(--agent "${LINGTAI_WORKBENCH_AGENT}")
  fi
  if [[ -n "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256:-}" ]]; then
    args+=(
      --accept-contract-sha256
      "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256}"
    )
  fi
  if [[ -n "${NOKV_EXPECTED_SHA256:-}" ]]; then
    args+=(--expected-sha256 "${NOKV_EXPECTED_SHA256}")
  fi
  python3 "${SCRIPT_DIR}/sync_workbench_mcp.py" "${args[@]}"
}

preflight_agent() {
  local project="$1"
  local args=(--preflight-only --project "${project}")
  if [[ -n "${LINGTAI_WORKBENCH_AGENT:-}" ]]; then
    args+=(--agent "${LINGTAI_WORKBENCH_AGENT}")
  fi
  if [[ -n "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256:-}" ]]; then
    args+=(
      --accept-contract-sha256
      "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256}"
    )
  fi
  python3 "${SCRIPT_DIR}/sync_workbench_mcp.py" "${args[@]}"
}

check_runtime_skill() {
  [[ -x "${TUI_PYTHON}" ]] || die "LingTai TUI runtime python not found: ${TUI_PYTHON}"
  "${TUI_PYTHON}" - <<'PY' || die "LingTai TUI runtime does not expose intrinsic skill nokv-workbench; install a workbench-enabled LingTai runtime first"
from pathlib import Path
import lingtai.intrinsic_skills as skills

root = Path(skills.__file__).parent
if not (root / "nokv-workbench" / "SKILL.md").exists():
    raise SystemExit(1)
PY
  log "LingTai runtime skill ready"
}

validate_guarded_credentials() {
  local access_key="${LINGTAI_WORKBENCH_S3_ACCESS_KEY_ID:-rustfsadmin}"
  local secret_key="${LINGTAI_WORKBENCH_S3_SECRET_ACCESS_KEY:-rustfsadmin}"
  if [[ "${access_key}" != "rustfsadmin" || "${secret_key}" != "rustfsadmin" ]]; then
    die "up.sh supports the dedicated local RustFS credentials only; custom credential deployment is outside this guarded helper"
  fi
}

nokv_ls() {
  "${NOKV_BIN}" \
    --server-bind "${SERVER_BIND}" \
    --object-backend "${OBJECT_BACKEND}" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${S3_BUCKET}" \
    ls / >/dev/null
}

port_in_use() {
  local host="${SERVER_BIND%:*}"
  local port="${SERVER_BIND##*:}"
  lsof -nP -iTCP@"${host}:${port}" -sTCP:LISTEN >/dev/null 2>&1
}

verify_managed_server() {
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" verify --state "${SERVER_STATE}"
}

verify_reusable_server() {
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" verify \
    --state "${SERVER_STATE}" \
    --expect-binary "${NOKV_BIN}" \
    --expect-server-bind "${SERVER_BIND}" \
    --expect-meta "${META_DIR}" \
    --expect-object-backend "${OBJECT_BACKEND}" \
    --expect-s3-endpoint "${S3_ENDPOINT}" \
    --expect-s3-bucket "${S3_BUCKET}" \
    -- "${SERVER_ARGV[@]}"
}

managed_server_pid() {
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" pid --state "${SERVER_STATE}"
}

stop_managed_server() {
  local pid=""
  if ! pid="$(managed_server_pid)"; then
    die "managed server state is invalid and cannot be terminated: ${SERVER_STATE}"
  fi
  log "stopping managed NoKV server pid=${pid}"
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" terminate \
    --state "${SERVER_STATE}" \
    --timeout-seconds 5 >/dev/null ||
    die "cannot safely terminate the managed NoKV server recorded in ${SERVER_STATE}"
  rm -f "${SERVER_PID}" "${SERVER_STATE}"
}

cleanup_started_server() {
  local pid="$1"
  local recorded_pid=""
  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" 2>/dev/null || true
  fi
  if [[ -f "${SERVER_PID}" ]] && [[ "$(cat "${SERVER_PID}")" == "${pid}" ]]; then
    rm -f "${SERVER_PID}"
  fi
  recorded_pid="$(managed_server_pid 2>/dev/null || true)"
  if [[ "${recorded_pid}" == "${pid}" ]]; then
    rm -f "${SERVER_STATE}"
  fi
}

record_started_server() {
  local pid="$1"
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" write \
    --state "${SERVER_STATE}" \
    --pid "${pid}" \
    --binary "${NOKV_BIN}" \
    --server-bind "${SERVER_BIND}" \
    --meta "${META_DIR}" \
    --object-backend "${OBJECT_BACKEND}" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${S3_BUCKET}" \
    -- "${SERVER_ARGV[@]}" >/dev/null
}

start_nokv_server() {
  mkdir -p "${STATE_DIR}" "${META_DIR}"
  log "starting NoKV server at ${SERVER_BIND}"
  python3 "${SCRIPT_DIR}/managed_nokv_server.py" launch \
    --state "${SERVER_STATE}" \
    --binary "${NOKV_BIN}" \
    --server-bind "${SERVER_BIND}" \
    --meta "${META_DIR}" \
    --object-backend "${OBJECT_BACKEND}" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${S3_BUCKET}" \
    -- "${SERVER_ARGV[@]}" >"${SERVER_LOG}" 2>&1 &
  local pid="$!"
  local pid_tmp="${SERVER_PID}.tmp.$$"
  printf '%s\n' "${pid}" >"${pid_tmp}"
  mv "${pid_tmp}" "${SERVER_PID}"

  for _ in $(seq 1 60); do
    if nokv_ls; then
      if ! record_started_server "${pid}"; then
        cleanup_started_server "${pid}"
        die "NoKV server became ready but its launch identity could not be recorded"
      fi
      log "NoKV server ready pid=${pid}"
      return
    fi
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      tail -80 "${SERVER_LOG}" >&2 || true
      cleanup_started_server "${pid}"
      die "NoKV server exited before becoming ready"
    fi
    sleep 1
  done

  tail -80 "${SERVER_LOG}" >&2 || true
  cleanup_started_server "${pid}"
  die "NoKV server did not become ready at ${SERVER_BIND}"
}

ensure_nokv_server() {
  if [[ -e "${SERVER_STATE}" || -L "${SERVER_STATE}" ]]; then
    if verify_reusable_server >/dev/null 2>&1; then
      if nokv_ls; then
        log "NoKV server already ready at ${SERVER_BIND} with the locked runtime"
        return
      fi
      log "managed NoKV server is not healthy; restarting it"
      stop_managed_server
    elif verify_managed_server >/dev/null 2>&1; then
      stop_managed_server
    else
      local recorded_pid=""
      recorded_pid="$(managed_server_pid 2>/dev/null || true)"
      if [[ ! "${recorded_pid}" =~ ^[0-9]+$ ]]; then
        managed_server_pid >&2 || true
        die "managed server state is invalid and requires manual inspection: ${SERVER_STATE}"
      fi
      if kill -0 "${recorded_pid}" >/dev/null 2>&1; then
        verify_managed_server >&2 || true
        die "managed server state is unsafe to reuse or stop: ${SERVER_STATE}"
      fi
      log "removing stale managed server state"
      rm -f "${SERVER_PID}" "${SERVER_STATE}"
    fi
  fi
  if port_in_use; then
    lsof -nP -iTCP@"${SERVER_BIND}" -sTCP:LISTEN >&2 || true
    die "${SERVER_BIND} is occupied, but NoKV client preflight failed; stop the conflicting process or change LINGTAI_WORKBENCH_SERVER_BIND"
  fi
  start_nokv_server
}

main() {
  [[ "$#" -eq 0 ]] || die "up.sh accepts no arguments; configure it with LINGTAI_WORKBENCH_* environment variables"
  require_cmd python3
  require_cmd lsof
  acquire_up_lock

  local project
  project="$(resolve_project)"
  [[ -d "${project}/.lingtai" ]] || die "not a LingTai project: ${project}"

  log "project: ${project}"
  check_runtime_skill
  preflight_agent "${project}"
  validate_guarded_credentials
  prepare_runtime "${project}"

  export AWS_ACCESS_KEY_ID="rustfsadmin"
  export AWS_SECRET_ACCESS_KEY="rustfsadmin"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  export AWS_EC2_METADATA_DISABLED=true
  "${SCRIPT_DIR}/start_rustfs.sh"
  if port_in_use; then
    log "validating candidate MCP contract before server handoff"
    probe_candidate_contract "${project}"
  fi
  ensure_nokv_server

  local sync_args=(
    --project "${project}"
    --nokv-bin "${NOKV_BIN}"
    "${RUNTIME_IDENTITY_ARGS[@]}"
    --server-bind "${SERVER_BIND}"
    --object-backend "${OBJECT_BACKEND}"
    --s3-endpoint "${S3_ENDPOINT}"
    --s3-bucket "${S3_BUCKET}"
    --workbench-root "${WORKBENCH_ROOT}"
  )
  if [[ -n "${LINGTAI_WORKBENCH_AGENT:-}" ]]; then
    sync_args+=(--agent "${LINGTAI_WORKBENCH_AGENT}")
  fi
  if [[ -n "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256:-}" ]]; then
    sync_args+=(
      --accept-contract-sha256
      "${LINGTAI_WORKBENCH_ACCEPT_CONTRACT_SHA256}"
    )
  fi
  if [[ -n "${NOKV_EXPECTED_SHA256:-}" ]]; then
    sync_args+=(--expected-sha256 "${NOKV_EXPECTED_SHA256}")
  fi
  python3 "${SCRIPT_DIR}/sync_workbench_mcp.py" "${sync_args[@]}"

  cat <<EOF

LingTai workbench is ready.
Run /refresh in the target LingTai agent.

Defaults used:
  project:        ${project}
  server_bind:    ${SERVER_BIND}
  s3_endpoint:    ${S3_ENDPOINT}
  s3_bucket:      ${S3_BUCKET}
  workbench_root: ${WORKBENCH_ROOT}
EOF
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
