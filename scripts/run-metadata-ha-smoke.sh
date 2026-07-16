#!/usr/bin/env bash
#
# Metadata HA smoke test against local RustFS plus etcd.
#
# This proves the deployable control-plane path: a first nokv server owns the
# shard through etcd, archives a checkpoint and sync shared-log segment to the
# object store, then a replacement server acquires the next epoch after the
# first owner dies and verifies checkpoint+log recovery.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
ETCD_BIN="${NOKV_ETCD_BIN:-etcd}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
CURL_BIN="${NOKV_CURL_BIN:-curl}"
PYTHON_BIN="${NOKV_PYTHON_BIN:-python3}"

RUSTFS_ADDRESS="${NOKV_HA_RUSTFS_ADDRESS:-127.0.0.1:9030}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_HA_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9031}"
RUSTFS_ENDPOINT="${NOKV_HA_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET_BASE="${NOKV_HA_RUSTFS_BUCKET:-nokv-ha-smoke}"
RUSTFS_BUCKET=""
RUSTFS_ACCESS_KEY="${NOKV_HA_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_HA_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_HA_RUSTFS_BUFFER_PROFILE:-AiTraining}"
EXTERNAL_RUSTFS="${NOKV_HA_EXTERNAL_RUSTFS:-0}"

ETCD_CLIENT_ADDRESS="${NOKV_HA_ETCD_CLIENT_ADDRESS:-127.0.0.1:12379}"
ETCD_PEER_ADDRESS="${NOKV_HA_ETCD_PEER_ADDRESS:-127.0.0.1:12380}"
ETCD_ENDPOINTS="${NOKV_HA_ETCD_ENDPOINTS:-http://${ETCD_CLIENT_ADDRESS}}"
ETCD_TTL_SECONDS="${NOKV_HA_ETCD_LEASE_TTL_SECONDS:-3}"

SERVER_BIND="${NOKV_HA_SERVER_BIND:-127.0.0.1:7730}"
SHARD_ID="${NOKV_HA_SHARD_ID:-mount-1:/}"
HA_CARGO_TARGET_DIR="${NOKV_HA_CARGO_TARGET_DIR:-$ROOT_DIR/target}"
STALE_OWNER_CHAOS="${NOKV_HA_STALE_OWNER_CHAOS:-0}"
KEEP_WORKDIR="${NOKV_HA_KEEP_WORKDIR:-0}"

WORK_DIR=""
RUSTFS_PID=""
ETCD_PID=""
SERVER_A_PID=""
SERVER_B_PID=""
OWN_ETCD=0
OWNER_A_READY_MS=0
OWNER_A_KILL_MS=0
OWNER_A_RESUME_MS=0
OWNER_B_START_MS=0
OWNER_B_READY_MS=0
VERIFY_DONE_MS=0
STALE_OWNER_DETECT_MS=0
STALE_OWNER_FENCE_MS=0
PRE_INODE=0
POST_INODE=0
AFTER_FAILOVER_INODE=0
CHECKPOINT_COMMIT_VERSION=0
RESTORE_SNAPSHOT_ID=0
RESTORE_CHECKPOINT_OPERATION=""
RESTORE_LOG_OPERATION=""
RESTORE_CALL_PID=""
RESTORE_BARRIER_PHASE="index-sealed"
RESTORE_REDRIVE_MAX_ATTEMPTS="${NOKV_HA_RESTORE_REDRIVE_MAX_ATTEMPTS:-256}"
BUCKET_CREATED=0

usage() {
    cat <<EOF
Usage: scripts/run-metadata-ha-smoke.sh

Environment:
  NOKV_HA_RUSTFS_ADDRESS              RustFS S3 address (default: 127.0.0.1:9030)
  NOKV_HA_RUSTFS_CONSOLE_ADDRESS      RustFS console address (default: 127.0.0.1:9031)
  NOKV_HA_RUSTFS_BUCKET               unique bucket name prefix (default: nokv-ha-smoke)
  NOKV_HA_EXTERNAL_RUSTFS=1            use the configured reachable RustFS endpoint instead of starting a local binary
  NOKV_HA_ETCD_ENDPOINTS              external etcd endpoints; when unset, start local etcd
  NOKV_HA_ETCD_CLIENT_ADDRESS         local etcd client address (default: 127.0.0.1:12379)
  NOKV_HA_ETCD_PEER_ADDRESS           local etcd peer address (default: 127.0.0.1:12380)
  NOKV_HA_ETCD_LEASE_TTL_SECONDS      owner lease TTL (default: 3)
  NOKV_HA_SERVER_BIND                 nokv server address (default: 127.0.0.1:7730)
  NOKV_HA_STALE_OWNER_CHAOS=1         pause owner A, fail over owner B on a second bind, and verify stale-owner fencing
  NOKV_HA_RESTORE_REDRIVE_MAX_ATTEMPTS bounded retries for retryable RestoreInProgress (default: 256)
  NOKV_HA_OWNER_A_BIND                owner A bind in stale-owner chaos mode (default: NOKV_HA_SERVER_BIND)
  NOKV_HA_OWNER_B_BIND                owner B bind in stale-owner chaos mode (default: 127.0.0.1:7731)
  NOKV_HA_METRICS_JSON                optional path for machine-readable timing output
  NOKV_HA_KEEP_WORKDIR=1              keep temporary logs and state

Requires rustfs, aws, curl, python3, and either etcd or NOKV_HA_ETCD_ENDPOINTS.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command not found: $cmd" >&2
        exit 127
    fi
}

now_ms() {
    if command -v python3 >/dev/null 2>&1; then
        python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
    else
        echo "$(($(date +%s) * 1000))"
    fi
}

extract_inode() {
    sed -n 's/.*inode=\([0-9][0-9]*\).*/\1/p'
}

json_field() {
    local field="$1"
    "$PYTHON_BIN" -c '
import json
import sys

field = sys.argv[1]
payload = json.load(sys.stdin)
if field not in payload or isinstance(payload[field], (dict, list, bool)):
    raise SystemExit(f"missing scalar JSON field: {field}")
print(payload[field])
' "$field"
}

restore_operation_id() {
    local snapshot_id="$1" destination="$2"
    "$PYTHON_BIN" -c '
import hashlib
import sys

snapshot = int(sys.argv[1])
source = b"/workbenches/ha-restore-source"
destination = ("/workbenches/" + sys.argv[2]).encode()
digest = hashlib.sha256()
digest.update(b"nokv-restore-to-fork-request-v1\0")
digest.update((1).to_bytes(8, "big"))
digest.update(len(source).to_bytes(8, "big"))
digest.update(source)
digest.update(snapshot.to_bytes(8, "big"))
digest.update(len(destination).to_bytes(8, "big"))
digest.update(destination)
print("restore-" + digest.hexdigest())
' "$snapshot_id" "$destination"
}

assert_fsck_clean() {
    local expected_complete="$1" expected_snapshot_pins="$2" expected_fork_bindings="$3"
    "$PYTHON_BIN" -c '
import json
import sys

expected_complete, expected_pins, expected_bindings = map(int, sys.argv[1:])
payload = json.load(sys.stdin)

def fail(message):
    raise SystemExit(message + ": " + repr(payload))

if payload.get("consistent") is not True:
    fail("fsck consistent is not true")
for count, rows in (("dangling_count", "dangling"), ("size_mismatch_count", "size_mismatches")):
    if payload.get(count) != 0 or payload.get(rows) != []:
        fail("fsck object-reference failure in " + count)
if payload.get("snapshot_pins_scanned") != expected_pins:
    fail("unexpected fsck snapshot pin count")
if payload.get("fork_bindings_scanned") != expected_bindings:
    fail("unexpected fsck ForkBinding count")
shards = payload.get("restore_shards")
if not isinstance(shards, list) or len(shards) != 1 or shards[0].get("mount_id") != 1:
    fail("fsck did not return exactly mount 1")
restore = shards[0].get("report")
if not isinstance(restore, dict) or restore.get("consistent") is not True:
    fail("restore fsck is not consistent")
for field in ("issues", "dangling_borrowed_objects", "borrowed_object_size_mismatches"):
    if restore.get(field) != []:
        fail("restore fsck has non-empty " + field)
metrics = restore.get("metrics")
if not isinstance(metrics, dict):
    fail("restore fsck metrics are absent")
if metrics.get("active_marker") is not True or metrics.get("allocator_v2_fenced") is not True:
    fail("restore downgrade fence is absent")
states = metrics.get("operations")
expected_states = {name: 0 for name in ("preparing", "ready_to_attach", "complete", "cleaning", "discarding", "releasing")}
expected_states["complete"] = expected_complete
if states != expected_states:
    fail("restore operation states are not uniquely Complete")
for field in ("cleanup_backlog", "release_backlog", "quarantine_rows"):
    if metrics.get(field) != 0:
        fail("restore backlog/quarantine is non-zero")
for field in ("staging_rows", "exact_reference_rows", "index_rows"):
    value = metrics.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        fail("complete restore graph lacks " + field)
control = metrics.get("control_rows")
if not isinstance(control, dict) or control.get("operation") != expected_complete:
    fail("restore control graph operation count is wrong")
if restore.get("borrowed_objects_checked", 0) <= 0:
    fail("restore fsck checked no borrowed objects")
print(json.dumps({"consistent": True, "complete": expected_complete}, separators=(",", ":")))
' "$expected_complete" "$expected_snapshot_pins" "$expected_fork_bindings"
}

assert_owner_b_recovered_inflight() {
    "$PYTHON_BIN" -c '
import json
import sys

payload = json.load(sys.stdin)
if payload.get("ready") is not True:
    raise SystemExit("owner B stats are not ready: {!r}".format(payload))
owner = payload.get("shard_owner")
if not isinstance(owner, dict) or owner.get("enabled") is not True:
    raise SystemExit("owner B shard_owner is absent: {!r}".format(payload))
if owner.get("node_id") != "node-b" or owner.get("epoch") != 2 or owner.get("state") != "serving":
    raise SystemExit("owner B identity/epoch/state is wrong: {!r}".format(owner))
restore = payload.get("restore")
if not isinstance(restore, dict) or restore.get("available") is not True:
    raise SystemExit("owner B restore metrics are unavailable: {!r}".format(restore))
expected = {
    "preparing": 1,
    "ready_to_attach": 0,
    "complete": 1,
    "cleaning": 0,
    "discarding": 0,
    "releasing": 0,
}
if restore.get("operations") != expected:
    raise SystemExit("owner B did not recover exactly one in-flight restore: {!r}".format(restore))
if restore.get("cleanup_backlog") != 0 or restore.get("quarantine_rows") != 0:
    raise SystemExit("owner B recovered restore backlog/quarantine: {!r}".format(restore))
print(json.dumps({"node_id": "node-b", "epoch": 2, "inflight": "Preparing"}, separators=(",", ":")))
'
}

mcp_call() {
    local bind="$1" tool="$2" arguments="$3" allow_restore_in_progress="${4:-0}"
    "$PYTHON_BIN" -c '
import json
import sys

print(json.dumps({
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {"name": sys.argv[1], "arguments": json.loads(sys.argv[2])},
}, separators=(",", ":")))
' "$tool" "$arguments" \
        | "$NOKV" --server-bind "$bind" "${S3_ARGS[@]}" \
            mcp --profile workbench --workbench-root /workbenches \
        | "$PYTHON_BIN" -c '
import json
import sys

responses = [json.loads(line) for line in sys.stdin if line.strip()]
if len(responses) != 1:
    raise SystemExit(f"expected one MCP response, received {len(responses)}")
response = responses[0]
if "error" in response:
    raise SystemExit("MCP protocol error: {!r}".format(response["error"]))
result = response.get("result")
if not isinstance(result, dict):
    raise SystemExit("MCP response has no result object")
structured = result.get("structuredContent")
if result.get("isError") is True:
    if (
        sys.argv[1] == "1"
        and isinstance(structured, dict)
        and structured.get("code") == "RestoreInProgress"
        and structured.get("retryable") is True
    ):
        print(json.dumps(structured, separators=(",", ":"), sort_keys=True))
        raise SystemExit(75)
    raise SystemExit(f"MCP tool error: {structured!r}")
if not isinstance(structured, dict):
    raise SystemExit("MCP result has no structuredContent object")
print(json.dumps(structured, separators=(",", ":"), sort_keys=True))
' "$allow_restore_in_progress"
}

mcp_restore_until_complete() {
    local bind="$1" arguments="$2" attempt payload status last_in_progress=""
    for ((attempt = 1; attempt <= RESTORE_REDRIVE_MAX_ATTEMPTS; attempt++)); do
        set +e
        payload="$(mcp_call "$bind" workbench_restore "$arguments" 1)"
        status=$?
        set -e
        if [[ "$status" -eq 0 ]]; then
            printf '%s\n' "$payload"
            return 0
        fi
        if [[ "$status" -ne 75 ]]; then
            return "$status"
        fi
        last_in_progress="$payload"
    done
    echo "error: restore remained retryable RestoreInProgress after ${RESTORE_REDRIVE_MAX_ATTEMPTS} identical redrive attempts" >&2
    if [[ -n "$last_in_progress" ]]; then
        echo "    $last_in_progress" >&2
    fi
    return 1
}

assert_restore_complete() {
    local payload="$1" expected_operation="$2" expected_destination="$3"
    printf '%s\n' "$payload" | "$PYTHON_BIN" -c '
import json
import sys

expected_operation, expected_destination = sys.argv[1:]
payload = json.load(sys.stdin)
if payload.get("status") != "success" or payload.get("state") != "complete":
    raise SystemExit(f"restore did not reach terminal Complete: {payload!r}")
if payload.get("cleanup_pending") is not False:
    raise SystemExit(f"restore reported cleanup_pending: {payload!r}")
if payload.get("operation_id") != expected_operation:
    raise SystemExit(
        "restore operation changed across recovery: {!r}".format(
            payload.get("operation_id")
        )
    )
if payload.get("destination_workbench_id") != expected_destination:
    raise SystemExit(f"restore destination changed: {payload!r}")
' "$expected_operation" "$expected_destination"
}

cleanup() {
    local status=$?
    if [[ -n "$RESTORE_CALL_PID" ]] && kill -0 "$RESTORE_CALL_PID" >/dev/null 2>&1; then
        kill "$RESTORE_CALL_PID" >/dev/null 2>&1 || true
        wait "$RESTORE_CALL_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_B_PID" ]] && kill -0 "$SERVER_B_PID" >/dev/null 2>&1; then
        kill "$SERVER_B_PID" >/dev/null 2>&1 || true
        wait "$SERVER_B_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$SERVER_A_PID" ]] && kill -0 "$SERVER_A_PID" >/dev/null 2>&1; then
        kill -CONT "$SERVER_A_PID" >/dev/null 2>&1 || true
        kill "$SERVER_A_PID" >/dev/null 2>&1 || true
        wait "$SERVER_A_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$BUCKET_CREATED" -eq 1 && -n "$RUSTFS_BUCKET" ]]; then
        AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3 rm "s3://${RUSTFS_BUCKET}" --recursive >/dev/null 2>&1 || true
        AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api delete-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1 || true
    fi
    if [[ "$OWN_ETCD" -eq 1 && -n "$ETCD_PID" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then
        kill "$ETCD_PID" >/dev/null 2>&1 || true
        wait "$ETCD_PID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$WORK_DIR" ]]; then
        for log in rustfs.log etcd.log server-a.log server-b.log; do
            if [[ -f "$WORK_DIR/$log" ]]; then
                echo "---- $log tail ----" >&2
                tail -80 "$WORK_DIR/$log" >&2 || true
            fi
        done
    fi
    if [[ -n "$WORK_DIR" && "$KEEP_WORKDIR" == "1" ]]; then
        echo "HA smoke workdir: $WORK_DIR" >&2
    elif [[ -n "$WORK_DIR" ]]; then
        rm -rf "$WORK_DIR"
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

wait_for_url() {
    local url="$1" name="$2" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        "$CURL_BIN" -fsS --max-time 2 "$url" >/dev/null 2>&1 && return 0
        "$CURL_BIN" -sS -I --max-time 2 "$url" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $url" >&2
    return 1
}

wait_for_path() {
    local path="$1" pid="$2" name="$3" deadline=$((SECONDS + 120))
    while ((SECONDS < deadline)); do
        [[ -f "$path" ]] && return 0
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: $name exited before publishing $path" >&2
            return 1
        fi
        sleep 0.05
    done
    echo "error: timed out waiting for $name at $path" >&2
    return 1
}

wait_for_server() {
    local bind="$1" pid="$2" name="$3" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: $name exited before becoming ready" >&2
            return 1
        fi
        "$CURL_BIN" -fsS --max-time 2 "http://${bind}/readyz" >/dev/null 2>&1 && return 0
        sleep 0.25
    done
    echo "error: timed out waiting for $name at $bind" >&2
    return 1
}

wait_for_stale_owner_fence() {
    local bind="$1" pid="$2" deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -n "$pid" ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
            echo "error: stale owner exited before observing the new epoch" >&2
            return 1
        fi
        local stats
        stats="$("$CURL_BIN" -fsS --max-time 2 "http://${bind}/stats" 2>/dev/null || true)"
        if echo "$stats" | grep -q '"last_error":"lease holder does not own shard'; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for stale owner to observe the new epoch" >&2
    return 1
}

create_bucket() {
    local deadline=$((SECONDS + 30))
    if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
        AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
        "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
        s3api head-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
        echo "error: supposedly unique HA bucket already exists: $RUSTFS_BUCKET" >&2
        return 1
    fi
    while ((SECONDS < deadline)); do
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api create-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
            BUCKET_CREATED=1
            return 0
        fi
        sleep 0.5
    done
    echo "error: failed to create or find bucket '$RUSTFS_BUCKET' at $RUSTFS_ENDPOINT" >&2
    return 1
}

if ! [[ "$ETCD_TTL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: NOKV_HA_ETCD_LEASE_TTL_SECONDS must be a positive integer" >&2
    exit 2
fi
if [[ "$STALE_OWNER_CHAOS" != "0" && "$STALE_OWNER_CHAOS" != "1" ]]; then
    echo "error: NOKV_HA_STALE_OWNER_CHAOS must be 0 or 1" >&2
    exit 2
fi
if [[ "$EXTERNAL_RUSTFS" != "0" && "$EXTERNAL_RUSTFS" != "1" ]]; then
    echo "error: NOKV_HA_EXTERNAL_RUSTFS must be 0 or 1" >&2
    exit 2
fi
if ! [[ "$RESTORE_REDRIVE_MAX_ATTEMPTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: NOKV_HA_RESTORE_REDRIVE_MAX_ATTEMPTS must be a positive integer" >&2
    exit 2
fi

OWNER_A_BIND="${NOKV_HA_OWNER_A_BIND:-$SERVER_BIND}"
OWNER_B_BIND="$SERVER_BIND"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    OWNER_B_BIND="${NOKV_HA_OWNER_B_BIND:-127.0.0.1:7731}"
fi

if [[ "$EXTERNAL_RUSTFS" == "0" ]]; then
    require_cmd "$RUSTFS_BIN"
fi
require_cmd "$AWS_BIN"
require_cmd "$CURL_BIN"
require_cmd "$PYTHON_BIN"
if [[ -z "${NOKV_HA_ETCD_ENDPOINTS:-}" ]]; then
    require_cmd "$ETCD_BIN"
    OWN_ETCD=1
fi

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-ha-smoke.XXXXXX")"
mkdir -p "$WORK_DIR/rustfs" "$WORK_DIR/etcd" "$WORK_DIR/meta-a" "$WORK_DIR/meta-b"

UNIQUE="$(date +%s)-$$"
RUSTFS_BUCKET="${RUSTFS_BUCKET_BASE:0:40}-${UNIQUE}"
ETCD_PREFIX="${NOKV_HA_ETCD_PREFIX:-/nokv/ha-smoke/${UNIQUE}}"
CHECKPOINT_PREFIX="${NOKV_HA_CHECKPOINT_PREFIX:-metadata/ha-smoke/${UNIQUE}/checkpoints}"
SHARED_LOG_PREFIX="${NOKV_HA_SHARED_LOG_PREFIX:-metadata/ha-smoke/${UNIQUE}/shared-log}"
RESTORE_BARRIER_DIR="$WORK_DIR/restore-barriers"
mkdir -p "$RESTORE_BARRIER_DIR"

S3_ARGS=(
    --object-backend rustfs
    --s3-bucket "$RUSTFS_BUCKET"
    --s3-endpoint "$RUSTFS_ENDPOINT"
    --s3-access-key-id "$RUSTFS_ACCESS_KEY"
    --s3-secret-access-key "$RUSTFS_SECRET_KEY"
)
CONTROL_ARGS=(
    --control-backend etcd
    --control-etcd-endpoints "$ETCD_ENDPOINTS"
    --control-etcd-prefix "$ETCD_PREFIX"
    --control-etcd-lease-ttl-seconds "$ETCD_TTL_SECONDS"
    --shard-id "$SHARD_ID"
    --shard-owner-renewal-interval-ms 500
    --metadata-shared-log-prefix "$SHARED_LOG_PREFIX"
    --metadata-checkpoint-archive-prefix "$CHECKPOINT_PREFIX"
)
CLIENT_A=(--server-bind "$OWNER_A_BIND" "${S3_ARGS[@]}")
CLIENT_B=(--server-bind "$OWNER_B_BIND" "${S3_ARGS[@]}")

echo "==> building nokv with etcd feature"
(
    cd "$ROOT_DIR"
    CARGO_TARGET_DIR="$HA_CARGO_TARGET_DIR" cargo build -p nokv --features etcd >/dev/null
)
NOKV="$HA_CARGO_TARGET_DIR/debug/nokv"

if [[ "$EXTERNAL_RUSTFS" == "0" ]]; then
    echo "==> starting RustFS at $RUSTFS_ENDPOINT"
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
        RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
        "$RUSTFS_BIN" server \
        --address "$RUSTFS_ADDRESS" \
        --console-enable \
        --console-address "$RUSTFS_CONSOLE_ADDRESS" \
        --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
        "$WORK_DIR/rustfs" >"$WORK_DIR/rustfs.log" 2>&1 &
    RUSTFS_PID=$!
else
    echo "==> using external RustFS at $RUSTFS_ENDPOINT"
fi
wait_for_url "$RUSTFS_ENDPOINT" RustFS
create_bucket

if [[ "$OWN_ETCD" -eq 1 ]]; then
    echo "==> starting etcd at $ETCD_ENDPOINTS"
    "$ETCD_BIN" \
        --name nokv-ha-smoke \
        --data-dir "$WORK_DIR/etcd" \
        --listen-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --advertise-client-urls "http://${ETCD_CLIENT_ADDRESS}" \
        --listen-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-advertise-peer-urls "http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster "nokv-ha-smoke=http://${ETCD_PEER_ADDRESS}" \
        --initial-cluster-state new \
        --initial-cluster-token "nokv-ha-smoke-${UNIQUE}" \
        >"$WORK_DIR/etcd.log" 2>&1 &
    ETCD_PID=$!
fi
FIRST_ETCD_ENDPOINT="${ETCD_ENDPOINTS%%,*}"
wait_for_url "${FIRST_ETCD_ENDPOINT%/}/health" etcd

echo "==> starting owner A with etcd lease and sync shared-log"
NOKV_TEST_RESTORE_BARRIER_DIR="$RESTORE_BARRIER_DIR" \
NOKV_TEST_BARRIER_TIMEOUT_MS=300000 \
"$NOKV" \
    --meta "$WORK_DIR/meta-a" \
    --server-bind "$OWNER_A_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_ARGS[@]}" \
    --node-id node-a \
    serve >"$WORK_DIR/server-a.log" 2>&1 &
SERVER_A_PID=$!
wait_for_server "$OWNER_A_BIND" "$SERVER_A_PID" "nokv owner A"
OWNER_A_READY_MS="$(now_ms)"

printf 'ha-smoke-pre-checkpoint' >"$WORK_DIR/pre.bin"
printf 'ha-smoke-post-checkpoint' >"$WORK_DIR/post.bin"

echo "==> writing data before checkpoint"
"$NOKV" "${CLIENT_A[@]}" mkdir /runs
PRE_OUT="$("$NOKV" "${CLIENT_A[@]}" put-artifact /runs/pre.bin "$WORK_DIR/pre.bin")"
echo "$PRE_OUT"
PRE_INODE="$(printf '%s\n' "$PRE_OUT" | extract_inode)"
"$NOKV" "${CLIENT_A[@]}" ls /runs | grep -q "pre.bin"

echo "==> creating a durable workbench restore before checkpoint"
mcp_call "$OWNER_A_BIND" workbench_create \
    '{"id":"ha-restore-source"}' >/dev/null
mcp_call "$OWNER_A_BIND" workbench_put_file \
    '{"id":"ha-restore-source","section":"outputs","path":"result.txt","text":"ha-restore-source\n"}' >/dev/null
mcp_call "$OWNER_A_BIND" workbench_commit \
    '{"id":"ha-restore-source","manifest":{"test":"metadata-ha-smoke"}}' >/dev/null
RESTORE_SNAPSHOT_JSON="$(mcp_call "$OWNER_A_BIND" workbench_snapshot \
    '{"id":"ha-restore-source","name":"failover","ttl_days":7}')"
RESTORE_SNAPSHOT_ID="$(printf '%s\n' "$RESTORE_SNAPSHOT_JSON" | json_field snapshot_id)"
RESTORE_CHECKPOINT_JSON="$(mcp_call "$OWNER_A_BIND" workbench_restore \
    "{\"id\":\"ha-restore-source\",\"at_snapshot\":${RESTORE_SNAPSHOT_ID},\"destination_id\":\"ha-restore-checkpoint\"}")"
RESTORE_CHECKPOINT_OPERATION="$(printf '%s\n' "$RESTORE_CHECKPOINT_JSON" | json_field operation_id)"
assert_restore_complete \
    "$RESTORE_CHECKPOINT_JSON" "$RESTORE_CHECKPOINT_OPERATION" "ha-restore-checkpoint"

echo "==> publishing checkpoint ref through owner A"
BACKUP_JSON="$("$NOKV" "${CLIENT_A[@]}" backup)"
echo "    $BACKUP_JSON"
echo "$BACKUP_JSON" | grep -q '"checkpoint_key"'
CHECKPOINT_COMMIT_VERSION="$(printf '%s\n' "$BACKUP_JSON" | json_field commit_version)"

echo "==> writing data after checkpoint so failover must replay shared log"
POST_OUT="$("$NOKV" "${CLIENT_A[@]}" put-artifact /runs/post.bin "$WORK_DIR/post.bin")"
echo "$POST_OUT"
POST_INODE="$(printf '%s\n' "$POST_OUT" | extract_inode)"
"$NOKV" "${CLIENT_A[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

echo "==> holding a second durable restore in-flight in the post-checkpoint shared log"
RESTORE_LOG_OPERATION="$(restore_operation_id "$RESTORE_SNAPSHOT_ID" ha-restore-log)"
RESTORE_BARRIER_STEM="${RESTORE_LOG_OPERATION}.${RESTORE_BARRIER_PHASE}"
RESTORE_BARRIER_ARM="$RESTORE_BARRIER_DIR/${RESTORE_BARRIER_STEM}.arm"
RESTORE_BARRIER_READY="$RESTORE_BARRIER_DIR/${RESTORE_BARRIER_STEM}.ready"
RESTORE_BARRIER_CONTINUE="$RESTORE_BARRIER_DIR/${RESTORE_BARRIER_STEM}.continue"
rm -f "$RESTORE_BARRIER_READY" "$RESTORE_BARRIER_CONTINUE"
printf 'armed\n' >"$RESTORE_BARRIER_ARM"
(
    mcp_call "$OWNER_A_BIND" workbench_restore \
        "{\"id\":\"ha-restore-source\",\"at_snapshot\":${RESTORE_SNAPSHOT_ID},\"destination_id\":\"ha-restore-log\"}" \
        >"$WORK_DIR/inflight-restore.out" 2>"$WORK_DIR/inflight-restore.err"
) &
RESTORE_CALL_PID=$!
wait_for_path "$RESTORE_BARRIER_READY" "$RESTORE_CALL_PID" "in-flight restore"
if [[ -s "$WORK_DIR/inflight-restore.out" ]]; then
    echo "error: in-flight restore returned before failover" >&2
    cat "$WORK_DIR/inflight-restore.out" >&2
    exit 1
fi

if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "==> pausing owner A and waiting for etcd lease expiry"
else
    echo "==> killing owner A and waiting for etcd lease expiry"
fi
OWNER_A_KILL_MS="$(now_ms)"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    kill -STOP "$SERVER_A_PID" >/dev/null 2>&1
else
    kill "$SERVER_A_PID" >/dev/null 2>&1 || true
    wait "$SERVER_A_PID" >/dev/null 2>&1 || true
    SERVER_A_PID=""
fi
if [[ -n "$RESTORE_CALL_PID" ]] && kill -0 "$RESTORE_CALL_PID" >/dev/null 2>&1; then
    kill "$RESTORE_CALL_PID" >/dev/null 2>&1 || true
    wait "$RESTORE_CALL_PID" >/dev/null 2>&1 || true
fi
RESTORE_CALL_PID=""
rm -f "$RESTORE_BARRIER_ARM" "$RESTORE_BARRIER_READY"
printf 'continue\n' >"$RESTORE_BARRIER_CONTINUE"
sleep "$((ETCD_TTL_SECONDS + 2))"

echo "==> starting owner B as failover from epoch 1"
OWNER_B_START_MS="$(now_ms)"
NOKV_TEST_RESTORE_BARRIER_DIR="$RESTORE_BARRIER_DIR" \
NOKV_TEST_BARRIER_TIMEOUT_MS=300000 \
"$NOKV" \
    --meta "$WORK_DIR/meta-b" \
    --server-bind "$OWNER_B_BIND" \
    "${S3_ARGS[@]}" \
    "${CONTROL_ARGS[@]}" \
    --node-id node-b \
    --failover-from-epoch 1 \
    serve >"$WORK_DIR/server-b.log" 2>&1 &
SERVER_B_PID=$!
wait_for_server "$OWNER_B_BIND" "$SERVER_B_PID" "nokv owner B"
OWNER_B_READY_MS="$(now_ms)"

STATS="$("$CURL_BIN" -fsS "http://${OWNER_B_BIND}/stats")"
OWNER_B_RECOVERY_SUMMARY="$(printf '%s\n' "$STATS" | assert_owner_b_recovered_inflight)"
echo "    $OWNER_B_RECOVERY_SUMMARY"

echo "==> verifying checkpoint restore and shared-log replay"
"$NOKV" "${CLIENT_B[@]}" cat /runs/pre.bin | grep -q "ha-smoke-pre-checkpoint"
"$NOKV" "${CLIENT_B[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

echo "==> verifying Complete restore from checkpoint and redriving in-flight log restore"
RECOVERED_CHECKPOINT_JSON="$(mcp_call "$OWNER_B_BIND" workbench_restore \
    "{\"id\":\"ha-restore-source\",\"at_snapshot\":${RESTORE_SNAPSHOT_ID},\"destination_id\":\"ha-restore-checkpoint\"}")"
assert_restore_complete \
    "$RECOVERED_CHECKPOINT_JSON" "$RESTORE_CHECKPOINT_OPERATION" "ha-restore-checkpoint"
RECOVERED_LOG_JSON="$(mcp_restore_until_complete "$OWNER_B_BIND" \
    "{\"id\":\"ha-restore-source\",\"at_snapshot\":${RESTORE_SNAPSHOT_ID},\"destination_id\":\"ha-restore-log\"}")"
assert_restore_complete "$RECOVERED_LOG_JSON" "$RESTORE_LOG_OPERATION" "ha-restore-log"
"$NOKV" "${CLIENT_B[@]}" \
    cat /workbenches/ha-restore-checkpoint/outputs/result.txt \
    | grep -q "ha-restore-source"
"$NOKV" "${CLIENT_B[@]}" \
    cat /workbenches/ha-restore-log/outputs/result.txt \
    | grep -q "ha-restore-source"

echo "==> retiring the source pin and verifying terminal restore redrive"
"$NOKV" "${CLIENT_B[@]}" retire-snapshot \
    /workbenches/ha-restore-source "$RESTORE_SNAPSHOT_ID" \
    | grep -q 'retired=true'
REDRIVEN_LOG_JSON="$(mcp_call "$OWNER_B_BIND" workbench_restore \
    "{\"id\":\"ha-restore-source\",\"at_snapshot\":${RESTORE_SNAPSHOT_ID},\"destination_id\":\"ha-restore-log\"}")"
assert_restore_complete "$REDRIVEN_LOG_JSON" "$RESTORE_LOG_OPERATION" "ha-restore-log"

echo "==> verifying owner B accepts new writes without clobbering replayed data"
AFTER_OUT="$("$NOKV" "${CLIENT_B[@]}" mkdir /after-failover)"
echo "$AFTER_OUT"
AFTER_FAILOVER_INODE="$(printf '%s\n' "$AFTER_OUT" | extract_inode)"
if [[ -n "$POST_INODE" && -n "$AFTER_FAILOVER_INODE" && "$AFTER_FAILOVER_INODE" -le "$POST_INODE" ]]; then
    echo "error: post-failover inode $AFTER_FAILOVER_INODE did not advance past replayed inode $POST_INODE" >&2
    exit 1
fi
"$NOKV" "${CLIENT_B[@]}" ls / | grep -q "after-failover"
"$NOKV" "${CLIENT_B[@]}" ls /runs | grep -q "post.bin"
"$NOKV" "${CLIENT_B[@]}" cat /runs/post.bin | grep -q "ha-smoke-post-checkpoint"

echo "==> running fsck after failover"
FSCK_JSON="$("$NOKV" "${CLIENT_B[@]}" fsck)"
echo "    $FSCK_JSON"
FSCK_SUMMARY="$(printf '%s\n' "$FSCK_JSON" | assert_fsck_clean 2 0 0)"
echo "    strict $FSCK_SUMMARY"

if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "==> resuming owner A and verifying stale-owner fencing"
    OWNER_A_RESUME_MS="$(now_ms)"
    kill -CONT "$SERVER_A_PID" >/dev/null 2>&1
    wait_for_stale_owner_fence "$OWNER_A_BIND" "$SERVER_A_PID"
    STALE_OWNER_DETECT_MS="$(now_ms)"
    set +e
    STALE_WRITE_OUT="$("$NOKV" "${CLIENT_A[@]}" mkdir /stale-owner-write 2>&1)"
    STALE_WRITE_STATUS=$?
    set -e
    if [[ "$STALE_WRITE_STATUS" -eq 0 ]]; then
        echo "error: stale owner accepted a metadata write after epoch-2 failover" >&2
        echo "$STALE_WRITE_OUT" >&2
        exit 1
    fi
    echo "$STALE_WRITE_OUT" | grep -q "owner epoch 1 is stale; required owner epoch is 2"
    STALE_OWNER_FENCE_MS="$(now_ms)"
fi
VERIFY_DONE_MS="$(now_ms)"
rm -f "$RESTORE_BARRIER_ARM" "$RESTORE_BARRIER_READY" "$RESTORE_BARRIER_CONTINUE"

FAILOVER_OBSERVED_MS="$((OWNER_B_READY_MS - OWNER_A_KILL_MS))"
LEASE_WAIT_MS="$((OWNER_B_START_MS - OWNER_A_KILL_MS))"
OWNER_B_STARTUP_MS="$((OWNER_B_READY_MS - OWNER_B_START_MS))"
VERIFY_AFTER_READY_MS="$((VERIFY_DONE_MS - OWNER_B_READY_MS))"
STALE_OWNER_DETECT_AFTER_RESUME_MS=0
STALE_OWNER_FENCE_AFTER_DETECT_MS=0
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    STALE_OWNER_DETECT_AFTER_RESUME_MS="$((STALE_OWNER_DETECT_MS - OWNER_A_RESUME_MS))"
    STALE_OWNER_FENCE_AFTER_DETECT_MS="$((STALE_OWNER_FENCE_MS - STALE_OWNER_DETECT_MS))"
fi
METRICS_JSON="{\"lease_ttl_seconds\":${ETCD_TTL_SECONDS},\"stale_owner_chaos\":${STALE_OWNER_CHAOS},\"owner_a_ready_ms\":${OWNER_A_READY_MS},\"owner_a_kill_ms\":${OWNER_A_KILL_MS},\"owner_a_resume_ms\":${OWNER_A_RESUME_MS},\"owner_b_start_ms\":${OWNER_B_START_MS},\"owner_b_ready_ms\":${OWNER_B_READY_MS},\"failover_observed_ms\":${FAILOVER_OBSERVED_MS},\"lease_wait_ms\":${LEASE_WAIT_MS},\"owner_b_startup_ms\":${OWNER_B_STARTUP_MS},\"verify_after_ready_ms\":${VERIFY_AFTER_READY_MS},\"stale_owner_detect_after_resume_ms\":${STALE_OWNER_DETECT_AFTER_RESUME_MS},\"stale_owner_fence_after_detect_ms\":${STALE_OWNER_FENCE_AFTER_DETECT_MS},\"checkpoint_commit_version\":${CHECKPOINT_COMMIT_VERSION:-0},\"restore_snapshot_id\":${RESTORE_SNAPSHOT_ID:-0},\"restore_inflight_recovered\":true,\"restore_inflight_phase\":\"${RESTORE_BARRIER_PHASE}\",\"restore_inflight_operation\":\"${RESTORE_LOG_OPERATION}\",\"rustfs_bucket\":\"${RUSTFS_BUCKET}\",\"fsck_consistent\":true,\"pre_inode\":${PRE_INODE:-0},\"post_checkpoint_inode\":${POST_INODE:-0},\"after_failover_inode\":${AFTER_FAILOVER_INODE:-0}}"
if [[ -n "${NOKV_HA_METRICS_JSON:-}" ]]; then
    printf '%s\n' "$METRICS_JSON" >"$NOKV_HA_METRICS_JSON"
fi

echo
echo "HA_SMOKE_METRICS $METRICS_JSON"
if [[ "$STALE_OWNER_CHAOS" == "1" ]]; then
    echo "HA_STALE_OWNER_OK: resumed epoch-1 owner observed epoch 2 and rejected a stale write"
fi
echo "HA_SMOKE_OK: etcd owner failover restored checkpoint/log state, redrove an in-flight durable workbench restore, passed strict fsck, and served epoch 2"
