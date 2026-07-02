#!/usr/bin/env bash
set -euo pipefail

if ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null)"; then
  :
else
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
SCRIPT_DIR="${ROOT_DIR}/scripts/lingtai-workbench"
STATE_DIR="${ROOT_DIR}/target/lingtai-workbench"
NOKV_BIN="${NOKV_BIN:-${ROOT_DIR}/target/debug/nokv}"
SERVER_BIND="${LINGTAI_WORKBENCH_SERVER_BIND:-127.0.0.1:7799}"
S3_ENDPOINT="${LINGTAI_WORKBENCH_S3_ENDPOINT:-http://127.0.0.1:9000}"
S3_BUCKET="${LINGTAI_WORKBENCH_S3_BUCKET:-nokv-lingtai-workbench}"
OBJECT_BACKEND="${LINGTAI_WORKBENCH_OBJECT_BACKEND:-rustfs}"
WORKBENCH_ROOT="${LINGTAI_WORKBENCH_ROOT:-/agents/{agent_id}/wb}"
META_DIR="${LINGTAI_WORKBENCH_META_DIR:-${STATE_DIR}/meta}"
SERVER_LOG="${LINGTAI_WORKBENCH_SERVER_LOG:-${STATE_DIR}/nokv-server.log}"
SERVER_PID="${LINGTAI_WORKBENCH_SERVER_PID:-${STATE_DIR}/nokv-server.pid}"
TUI_PYTHON="${LINGTAI_TUI_PYTHON:-${HOME}/.lingtai-tui/runtime/venv/bin/python}"

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

start_nokv_server() {
  mkdir -p "${STATE_DIR}" "${META_DIR}"
  log "starting NoKV server at ${SERVER_BIND}"
  "${NOKV_BIN}" \
    --server-bind "${SERVER_BIND}" \
    --object-backend "${OBJECT_BACKEND}" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${S3_BUCKET}" \
    --meta "${META_DIR}" \
    serve >"${SERVER_LOG}" 2>&1 &
  echo "$!" >"${SERVER_PID}"

  for _ in $(seq 1 60); do
    if nokv_ls; then
      log "NoKV server ready pid=$(cat "${SERVER_PID}")"
      return
    fi
    if ! kill -0 "$(cat "${SERVER_PID}")" >/dev/null 2>&1; then
      tail -80 "${SERVER_LOG}" >&2 || true
      die "NoKV server exited before becoming ready"
    fi
    sleep 1
  done

  tail -80 "${SERVER_LOG}" >&2 || true
  die "NoKV server did not become ready at ${SERVER_BIND}"
}

ensure_nokv_server() {
  if nokv_ls; then
    log "NoKV server already ready at ${SERVER_BIND}"
    return
  fi
  if port_in_use; then
    lsof -nP -iTCP@"${SERVER_BIND}" -sTCP:LISTEN >&2 || true
    die "${SERVER_BIND} is occupied, but NoKV client preflight failed; stop the conflicting process or change LINGTAI_WORKBENCH_SERVER_BIND"
  fi
  start_nokv_server
}

check_mcp_tools() {
  local names
  names="$(
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' |
      "${NOKV_BIN}" \
        --server-bind "${SERVER_BIND}" \
        --object-backend "${OBJECT_BACKEND}" \
        --s3-endpoint "${S3_ENDPOINT}" \
        --s3-bucket "${S3_BUCKET}" \
        mcp --profile workbench --workbench-root "${WORKBENCH_ROOT}" |
      python3 -c 'import json, sys; print("\n".join(tool["name"] for tool in json.loads(sys.stdin.readline())["result"]["tools"]))'
  )"
  grep -qx 'workbench_put_file' <<<"${names}" || die "workbench_put_file is missing from MCP tools"
  if grep -q '^nokv_workbench_' <<<"${names}"; then
    die "old nokv_workbench_* tools are still exposed; rebuild target/debug/nokv"
  fi
  log "MCP tools ready"
}

main() {
  require_cmd cargo
  require_cmd python3
  require_cmd lsof

  local project
  project="$(resolve_project)"
  [[ -d "${project}/.lingtai" ]] || die "not a LingTai project: ${project}"

  log "project: ${project}"
  log "building nokv"
  cargo build -p nokv --bin nokv

  "${SCRIPT_DIR}/start_rustfs.sh"
  ensure_nokv_server
  check_runtime_skill
  check_mcp_tools

  "${SCRIPT_DIR}/install_workbench_mcp.py" \
    --project "${project}" \
    --nokv-bin "${NOKV_BIN}" \
    --server-bind "${SERVER_BIND}" \
    --object-backend "${OBJECT_BACKEND}" \
    --s3-endpoint "${S3_ENDPOINT}" \
    --s3-bucket "${S3_BUCKET}" \
    --workbench-root "${WORKBENCH_ROOT}"

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

main "$@"
