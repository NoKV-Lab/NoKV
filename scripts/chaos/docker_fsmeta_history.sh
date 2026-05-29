#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
source "$ROOT_DIR/scripts/lib/common.sh"

ADDR="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
MOUNT="${NOKV_FSMETA_MOUNT:-default}"
SEEDS="${NOKV_DOCKER_CHAOS_SEEDS:-3}"
STEPS="${NOKV_DOCKER_CHAOS_STEPS:-48}"
BATCH="${NOKV_DOCKER_CHAOS_BATCH:-3}"
TIMEOUT="${NOKV_DOCKER_CHAOS_TIMEOUT:-60s}"
READY_TIMEOUT="${NOKV_DOCKER_CHAOS_READY_TIMEOUT:-180}"
CHAOS_INTERVAL="${NOKV_DOCKER_CHAOS_INTERVAL:-5}"
ALLOW_INDETERMINATE="${NOKV_DOCKER_CHAOS_ALLOW_INDETERMINATE:-1}"
TOOLS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-chaos.XXXXXX")"
chaos_pid=""
history_pid=""

dump_cluster_state() {
  if [[ "${NOKV_DOCKER_CHAOS_DUMP_LOGS:-1}" != "1" ]]; then
    return
  fi
  docker compose ps || true
  docker compose logs --no-color --tail="${NOKV_DOCKER_CHAOS_LOG_TAIL:-200}" || true
}

stop_chaos() {
  if [[ -n "$chaos_pid" ]] && kill -0 "$chaos_pid" 2>/dev/null; then
    kill "$chaos_pid" 2>/dev/null || true
    wait "$chaos_pid" 2>/dev/null || true
  fi
  chaos_pid=""
  docker compose unpause coordinator-1 coordinator-2 coordinator-3 store-1 store-2 store-3 meta-root-1 meta-root-2 meta-root-3 fsmeta >/dev/null 2>&1 || true
  docker compose up -d --no-deps coordinator-1 coordinator-2 coordinator-3 store-1 store-2 store-3 meta-root-1 meta-root-2 meta-root-3 fsmeta >/dev/null 2>&1 || true
}

stop_history() {
  if [[ -n "$history_pid" ]] && kill -0 "$history_pid" 2>/dev/null; then
    kill "$history_pid" 2>/dev/null || true
    wait "$history_pid" 2>/dev/null || true
  fi
  history_pid=""
}

recover_service() {
  local service="$1"
  docker compose up -d --no-deps "$service"
}

cleanup() {
  local status="${1:-0}"
  if [[ "$status" != "0" ]]; then
    dump_cluster_state
  fi
  stop_history
  stop_chaos
  rm -rf "$TOOLS_DIR"
  if [[ "${NOKV_DOCKER_CHAOS_DOWN:-0}" == "1" ]]; then
    docker compose down -v
  fi
}
trap 'status=$?; cleanup "$status"; exit "$status"' EXIT
trap 'trap - EXIT; cleanup 130; exit 130' INT TERM

go build -o "$TOOLS_DIR/nokv-fsmeta-history" ./cmd/nokv-fsmeta-history
go build -o "$TOOLS_DIR/nokv-fsmeta-scrub" ./cmd/nokv-fsmeta-scrub

if [[ "${NOKV_DOCKER_CHAOS_BUILD:-1}" == "1" ]]; then
  docker compose up -d --build
else
  docker compose up -d
fi

wait_fsmeta() {
  nokv_wait_for_tcp "$ADDR" "$READY_TIMEOUT"
}

inject_fault() {
  local seed="$1"
  case $((seed % 14)) in
    0)
      docker compose restart fsmeta
      ;;
    1)
      docker compose restart coordinator-1
      ;;
    2)
      docker compose kill -s SIGKILL store-2
      recover_service store-2
      ;;
    3)
      docker compose restart meta-root-3
      ;;
    4)
      docker compose kill -s SIGKILL coordinator-2
      recover_service coordinator-2
      ;;
    5)
      docker compose restart store-3
      ;;
    6)
      docker compose pause store-1
      sleep 2
      docker compose unpause store-1
      ;;
    7)
      docker compose pause coordinator-3
      sleep 2
      docker compose unpause coordinator-3
      ;;
    8)
      docker compose restart meta-root-1
      docker compose restart meta-root-2
      ;;
    9)
      docker compose kill -s SIGKILL store-1
      recover_service store-1
      ;;
    10)
      isolate_service store-2 2
      ;;
    11)
      isolate_service coordinator-1 2
      ;;
    12)
      isolate_service meta-root-2 2
      ;;
    *)
      isolate_service fsmeta 2
      ;;
  esac
}

isolate_service() {
  local service="$1"
  local seconds="$2"
  local container
  container="$(docker compose ps -q "$service")"
  if [[ -z "$container" ]]; then
    return
  fi
  local network
  network="$(docker inspect -f '{{range $name, $network := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$container" | head -n 1)"
  if [[ -z "$network" ]]; then
    return
  fi
  docker network disconnect "$network" "$container" || true
  sleep "$seconds"
  docker network connect "$network" "$container" || true
}

inject_recovery_fault() {
  local seed="$1"
  case $((seed % 4)) in
    0)
      docker compose kill -s SIGKILL store-1
      recover_service store-1
      ;;
    1)
      docker compose restart store-2
      ;;
    2)
      docker compose restart coordinator-2
      ;;
    *)
      docker compose restart meta-root-3
      ;;
  esac
}

chaos_loop() {
  local seed=1
  while true; do
    sleep "$CHAOS_INTERVAL"
    inject_fault "$seed" || true
    wait_fsmeta || true
    seed=$((seed + 1))
  done
}

wait_history_ready() {
  local pid="$1"
  local ready_file="$2"
  local deadline=$((SECONDS + READY_TIMEOUT))
  while [[ ! -f "$ready_file" ]]; do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      return $?
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for fsmeta history scope preparation" >&2
      stop_history
      return 1
    fi
    sleep 1
  done
}

wait_fsmeta

history_args=(
  --addr "$ADDR"
  --mount "$MOUNT"
  --seed-start 1
  --seeds "$SEEDS"
  --steps "$STEPS"
  --batch "$BATCH"
  --timeout "$TIMEOUT"
)
if [[ "$ALLOW_INDETERMINATE" == "1" ]]; then
  # Process chaos can return Unavailable after a request crosses the service
  # boundary. The checker keeps both commit and no-commit candidates for those
  # operations instead of reporting a namespace-semantic mismatch.
  history_args+=(--allow-indeterminate-errors)
fi

if [[ "${NOKV_DOCKER_CHAOS_NEMESIS:-1}" == "1" ]]; then
  history_ready_file="$TOOLS_DIR/history.ready"
  inject_recovery_fault 1
  wait_fsmeta
  "$TOOLS_DIR/nokv-fsmeta-history" "${history_args[@]}" --ready-file "$history_ready_file" &
  history_pid="$!"
  wait_history_ready "$history_pid" "$history_ready_file"
  chaos_loop &
  chaos_pid="$!"
  wait "$history_pid"
  history_pid=""
else
  "$TOOLS_DIR/nokv-fsmeta-history" "${history_args[@]}"
fi

stop_chaos
wait_fsmeta
if [[ "${NOKV_DOCKER_CHAOS_SCRUB:-1}" == "1" ]]; then
  "$TOOLS_DIR/nokv-fsmeta-scrub" \
    --addr "$ADDR" \
    --mount "$MOUNT" \
    --timeout "${NOKV_DOCKER_CHAOS_SCRUB_TIMEOUT:-60s}" \
    --max-issues "${NOKV_DOCKER_CHAOS_SCRUB_MAX_ISSUES:-32}"
fi
