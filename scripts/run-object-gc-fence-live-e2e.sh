#!/usr/bin/env bash
#
# Run the durable object-reference/GC fence acceptance against live RustFS.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_BIN="${NOKV_RUSTFS_BIN:-rustfs}"
AWS_BIN="${NOKV_AWS_BIN:-aws}"
RUSTFS_ADDRESS="${NOKV_OBJECT_GC_E2E_RUSTFS_ADDRESS:-127.0.0.1:9050}"
RUSTFS_CONSOLE_ADDRESS="${NOKV_OBJECT_GC_E2E_RUSTFS_CONSOLE_ADDRESS:-127.0.0.1:9051}"
RUSTFS_ENDPOINT="${NOKV_OBJECT_GC_E2E_RUSTFS_ENDPOINT:-http://${RUSTFS_ADDRESS}}"
RUSTFS_BUCKET="${NOKV_OBJECT_GC_E2E_RUSTFS_BUCKET:-nokv-object-gc-fence-$$}"
RUSTFS_ACCESS_KEY="${NOKV_OBJECT_GC_E2E_RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${NOKV_OBJECT_GC_E2E_RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUFFER_PROFILE="${NOKV_OBJECT_GC_E2E_RUSTFS_BUFFER_PROFILE:-AiTraining}"
RUSTFS_RUN_PREFIX="${NOKV_OBJECT_GC_E2E_RUN_PREFIX:-nokv-object-gc-fence/$(date -u +%Y%m%dT%H%M%SZ)-$$}"
CARGO_TARGET_DIR_OVERRIDE="${NOKV_OBJECT_GC_E2E_CARGO_TARGET_DIR:-${CARGO_TARGET_DIR:-}}"
EXTERNAL_RUSTFS="${NOKV_OBJECT_GC_E2E_EXTERNAL_RUSTFS:-0}"

RUSTFS_DATA_DIR="${NOKV_OBJECT_GC_E2E_RUSTFS_DATA_DIR:-}"
RUSTFS_LOG="${NOKV_OBJECT_GC_E2E_RUSTFS_LOG:-}"
RUSTFS_PID=""
OWN_DATA_DIR=0

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "error: required command not found: $cmd" >&2
        exit 127
    fi
}

cleanup() {
    local status=$?
    if [[ -n "$RUSTFS_PID" ]] && kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
        kill "$RUSTFS_PID" >/dev/null 2>&1 || true
        wait "$RUSTFS_PID" >/dev/null 2>&1 || true
    fi
    if [[ "$status" -ne 0 && -n "$RUSTFS_LOG" && -f "$RUSTFS_LOG" ]]; then
        echo "---- RustFS log tail ----" >&2
        tail -80 "$RUSTFS_LOG" >&2 || true
        echo "-------------------------" >&2
    fi
    if [[ "$OWN_DATA_DIR" -eq 1 && "${NOKV_OBJECT_GC_E2E_KEEP_DATA:-0}" != "1" ]]; then
        rm -rf "$RUSTFS_DATA_DIR"
    elif [[ -n "$RUSTFS_DATA_DIR" ]]; then
        echo "RustFS data directory: $RUSTFS_DATA_DIR" >&2
    fi
    exit "$status"
}

wait_for_rustfs() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if [[ -n "$RUSTFS_PID" ]] && ! kill -0 "$RUSTFS_PID" >/dev/null 2>&1; then
            echo "error: RustFS exited before becoming ready" >&2
            return 1
        fi
        if curl -fsS --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1 \
            || curl -sS -I --max-time 2 "$RUSTFS_ENDPOINT" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: timed out waiting for RustFS at $RUSTFS_ENDPOINT" >&2
    return 1
}

create_bucket() {
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api create-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
            return 0
        fi
        if AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
            AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
            "$AWS_BIN" --endpoint-url "$RUSTFS_ENDPOINT" \
            s3api head-bucket --bucket "$RUSTFS_BUCKET" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo "error: failed to create or find bucket '$RUSTFS_BUCKET'" >&2
    return 1
}

if [[ "$EXTERNAL_RUSTFS" != "1" ]]; then
    require_cmd "$RUSTFS_BIN"
fi
require_cmd "$AWS_BIN"
require_cmd cargo
require_cmd curl

if [[ "$EXTERNAL_RUSTFS" != "1" ]]; then
    if [[ -z "$RUSTFS_DATA_DIR" ]]; then
        RUSTFS_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-object-gc-e2e.XXXXXX")"
        OWN_DATA_DIR=1
    else
        mkdir -p "$RUSTFS_DATA_DIR"
    fi
    if [[ -z "$RUSTFS_LOG" ]]; then
        RUSTFS_LOG="$RUSTFS_DATA_DIR/rustfs.log"
    fi
fi

trap cleanup EXIT INT TERM

if [[ "$EXTERNAL_RUSTFS" == "1" ]]; then
    echo "Using external RustFS at $RUSTFS_ENDPOINT"
else
    echo "Starting RustFS at $RUSTFS_ENDPOINT"
    RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
        RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
        "$RUSTFS_BIN" server \
        --address "$RUSTFS_ADDRESS" \
        --console-enable \
        --console-address "$RUSTFS_CONSOLE_ADDRESS" \
        --buffer-profile "$RUSTFS_BUFFER_PROFILE" \
        "$RUSTFS_DATA_DIR" >"$RUSTFS_LOG" 2>&1 &
    RUSTFS_PID=$!
fi

wait_for_rustfs
create_bucket

echo "Running object-reference/GC fence live acceptance"
test_args=(
    test
    -p nokv-client
    --test object_gc_fence_live
    --
    --ignored
    --nocapture
    --test-threads=1
)

(
    cd "$ROOT_DIR"
    if [[ -n "$CARGO_TARGET_DIR_OVERRIDE" ]]; then
        CARGO_TARGET_DIR="$CARGO_TARGET_DIR_OVERRIDE" \
            NOKV_OBJECT_GC_LIVE_ENDPOINT="$RUSTFS_ENDPOINT" \
            NOKV_OBJECT_GC_LIVE_BUCKET="$RUSTFS_BUCKET" \
            NOKV_OBJECT_GC_LIVE_RUN_PREFIX="$RUSTFS_RUN_PREFIX" \
            NOKV_OBJECT_GC_LIVE_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
            NOKV_OBJECT_GC_LIVE_SECRET_KEY="$RUSTFS_SECRET_KEY" \
            cargo "${test_args[@]}"
    else
        NOKV_OBJECT_GC_LIVE_ENDPOINT="$RUSTFS_ENDPOINT" \
            NOKV_OBJECT_GC_LIVE_BUCKET="$RUSTFS_BUCKET" \
            NOKV_OBJECT_GC_LIVE_RUN_PREFIX="$RUSTFS_RUN_PREFIX" \
            NOKV_OBJECT_GC_LIVE_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
            NOKV_OBJECT_GC_LIVE_SECRET_KEY="$RUSTFS_SECRET_KEY" \
            cargo "${test_args[@]}"
    fi
)
