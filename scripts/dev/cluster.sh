#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/config.sh"
source "$SCRIPT_DIR/../lib/workdir.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/dev/cluster.sh [options]

Launches the canonical 333 separated dev cluster:
  - 3 replicated meta-root peers (the Truth plane)
  - 1 coordinator bound to the remote meta-root cluster (the Service plane)
  - all stores declared in the raft config (the Execution plane)

Options:
  --config PATH              Raft configuration file (default: ./raft_config.example.json)
  --workdir DIR              Base directory for meta-root/coordinator/store data (default: ./artifacts/cluster)
  --coordinator-listen ADDR  Coordinator gRPC listen address override
  --coordinator-id ID        Stable coordinator lease owner id (default: c1)
  --root-refresh DURATION    Coordinator rooted refresh interval (default: 200ms)
  --lease-ttl DURATION       Coordinator lease ttl (default: 10s)
  --lease-renew-before DUR   Coordinator lease renew window (default: 3s)
  --raft-debug-log           Enable verbose raft debug logging for stores (default)
  --no-raft-debug-log        Disable verbose raft debug logging

Notes:
  - cluster.sh is a bootstrap/dev launcher, not a restart workflow.
  - Fresh store workdirs are seeded from config.regions; existing runtime workdirs are reused as-is.
  - For production-style restarts, run the ops scripts directly against the same durable workdirs:
      scripts/ops/serve-meta-root.sh --config ... --node-id ...
      scripts/ops/serve-coordinator.sh --coordinator-id ... --root-peer ...
      scripts/ops/serve-store.sh --config ... --store-id ...
USAGE
}

ROOT_DIR=$NOKV_ROOT_DIR
CONFIG_PATH="$ROOT_DIR/raft_config.example.json"
WORKDIR=""
COORDINATOR_LISTEN=""
COORDINATOR_ID="c1"
ROOT_REFRESH="200ms"
LEASE_TTL="10s"
LEASE_RENEW_BEFORE="3s"
RAFT_DEBUG=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG_PATH=$2
      shift 2
      ;;
    --workdir)
      WORKDIR=$2
      shift 2
      ;;
    --coordinator-listen)
      COORDINATOR_LISTEN=$2
      shift 2
      ;;
    --coordinator-id)
      COORDINATOR_ID=$2
      shift 2
      ;;
    --root-refresh)
      ROOT_REFRESH=$2
      shift 2
      ;;
    --lease-ttl)
      LEASE_TTL=$2
      shift 2
      ;;
    --lease-renew-before)
      LEASE_RENEW_BEFORE=$2
      shift 2
      ;;
    --raft-debug-log)
      RAFT_DEBUG=1
      shift
      ;;
    --no-raft-debug-log)
      RAFT_DEBUG=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "cluster.sh: unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if ! [[ -f "$CONFIG_PATH" ]]; then
  nokv_die "cluster.sh: configuration file not found: $CONFIG_PATH"
fi

if [[ -z "$WORKDIR" ]]; then
  WORKDIR="$ROOT_DIR/artifacts/cluster"
fi
mkdir -p "$WORKDIR"

nokv_build_cli_binaries
nokv_prepend_build_path

start_with_logs() {
  local __pid_var=$1
  local prefix=$2
  local logfile=$3
  shift 3

  "$@" > >(sed -u "s/^/[$prefix] /" | tee "$logfile") 2>&1 &
  printf -v "$__pid_var" '%s' "$!"
}

cleaned=0
ROOT_PIDS=()
STORE_PIDS=()
COORDINATOR_PID=""

cleanup() {
  if [[ $cleaned -eq 1 ]]; then
    return
  fi
  cleaned=1

  for pid in "${STORE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n "${COORDINATOR_PID:-}" ]] && kill -0 "$COORDINATOR_PID" 2>/dev/null; then
    kill -INT "$COORDINATOR_PID" 2>/dev/null || true
  fi
  for pid in "${ROOT_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done

  for pid in "${STORE_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
    fi
  done
  if [[ -n "${COORDINATOR_PID:-}" ]]; then
    wait "$COORDINATOR_PID" 2>/dev/null || true
  fi
  for pid in "${ROOT_PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      wait "$pid" || true
    fi
  done
}

trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

if [[ -z "$COORDINATOR_LISTEN" ]]; then
  COORDINATOR_LISTEN=$(nokv_config_coordinator_addr "$CONFIG_PATH" host)
fi
if [[ -z "$COORDINATOR_LISTEN" ]]; then
  COORDINATOR_LISTEN="127.0.0.1:2379"
fi

STORE_LINES=()
while IFS= read -r _line; do STORE_LINES+=("$_line"); done < <(nokv_config_store_lines "$CONFIG_PATH")
if [[ "${#STORE_LINES[@]}" -eq 0 ]]; then
  nokv_die "cluster.sh: no stores defined in $CONFIG_PATH"
fi

REGION_LINES=()
while IFS= read -r _line; do REGION_LINES+=("$_line"); done < <(nokv_config_region_lines "$CONFIG_PATH")
if [[ "${#REGION_LINES[@]}" -eq 0 ]]; then
  nokv_die "cluster.sh: no regions defined in $CONFIG_PATH"
fi

declare -a STORE_IDS STORE_WORKDIRS
for line in "${STORE_LINES[@]}"; do
  read -r store_id _listen _advertise _docker_listen _docker_addr _store_workdir _docker_workdir <<<"$line"
  STORE_IDS+=("$store_id")
  store_dir="$WORKDIR/store-$store_id"
  STORE_WORKDIRS+=("$store_dir")
  mkdir -p "$store_dir"
  nokv_remove_stale_lock_if_present "$store_dir"
done

for idx in "${!STORE_IDS[@]}"; do
  store_dir="${STORE_WORKDIRS[$idx]}"
  if [[ -f "$store_dir/CURRENT" ]]; then
    echo "Store ${STORE_IDS[$idx]} already bootstrapped; skipping local peer-catalog seeding"
    continue
  fi
  nokv_assert_fresh_workdir "$store_dir" "cluster.sh: store ${STORE_IDS[$idx]} has stale files; refusing to seed into dirty directory"
  for region_line in "${REGION_LINES[@]}"; do
    read -r region_id start_key end_key epoch_ver epoch_conf peer_str leader_store <<<"$region_line"
    args=(--workdir "$store_dir" --region-id "$region_id" --epoch-version "$epoch_ver" --epoch-conf-version "$epoch_conf")
    if [[ "$start_key" != "-" ]]; then
      args+=(--start-key "$start_key")
    fi
    if [[ "$end_key" != "-" ]]; then
      args+=(--end-key "$end_key")
    fi
    IFS=',' read -ra peers <<<"$peer_str"
    for peer in "${peers[@]}"; do
      if [[ -n "$peer" ]]; then
        args+=(--peer "$peer")
      fi
    done
    nokv-config catalog "${args[@]}"
  done
done

ROOT_NODE_IDS=()
ROOT_GRPC_ADDRS=()
while read -r node_id grpc_addr _transport_addr _root_workdir; do
  ROOT_NODE_IDS+=("$node_id")
  ROOT_GRPC_ADDRS+=("$grpc_addr")
done < <(nokv_config_meta_root_lines "$CONFIG_PATH" host)
if [[ "${#ROOT_NODE_IDS[@]}" -ne 3 ]]; then
  nokv_die "cluster.sh: meta_root.peers must contain exactly 3 peers"
fi

for idx in "${!ROOT_NODE_IDS[@]}"; do
  node_id="${ROOT_NODE_IDS[$idx]}"
  grpc_addr="${ROOT_GRPC_ADDRS[$idx]}"
  root_workdir="$WORKDIR/meta-root-$node_id"
  mkdir -p "$root_workdir"
  echo "Starting metadata root ${node_id} (grpc=${grpc_addr})"
  start_with_logs root_pid "meta-root-${node_id}" "$root_workdir/root.log" \
    "$ROOT_DIR/scripts/ops/serve-meta-root.sh" \
      --config "$CONFIG_PATH" \
      --scope host \
      --addr "$grpc_addr" \
      --workdir "$root_workdir" \
      --node-id "$node_id"
  ROOT_PIDS+=("$root_pid")
done

for grpc_addr in "${ROOT_GRPC_ADDRS[@]}"; do
  nokv_wait_for_tcp "$grpc_addr" 30
done

coordinator_root_peer_args=()
for idx in "${!ROOT_NODE_IDS[@]}"; do
  coordinator_root_peer_args+=(--root-peer "${ROOT_NODE_IDS[$idx]}=${ROOT_GRPC_ADDRS[$idx]}")
done

echo "Starting Coordinator service on ${COORDINATOR_LISTEN} (remote rooted mode)"
start_with_logs COORDINATOR_PID "coordinator" "$WORKDIR/coordinator.log" \
  "$ROOT_DIR/scripts/ops/serve-coordinator.sh" \
    --addr "$COORDINATOR_LISTEN" \
    --coordinator-id "$COORDINATOR_ID" \
    --root-refresh "$ROOT_REFRESH" \
    --lease-ttl "$LEASE_TTL" \
    --lease-renew-before "$LEASE_RENEW_BEFORE" \
    "${coordinator_root_peer_args[@]}"

nokv_wait_for_tcp "$COORDINATOR_LISTEN" 30

serve_debug_args=()
if [[ $RAFT_DEBUG -eq 1 ]]; then
  serve_debug_args=(--raft-debug-log)
fi

for idx in "${!STORE_IDS[@]}"; do
  store_id="${STORE_IDS[$idx]}"
  store_dir="${STORE_WORKDIRS[$idx]}"
  echo "Starting store ${store_id} (workdir=${store_dir})"
  serve_args=(
    --config "$CONFIG_PATH"
    --store-id "$store_id"
    --workdir "$store_dir"
    --coordinator-addr "$COORDINATOR_LISTEN"
    "${serve_debug_args[@]}"
  )
  start_with_logs store_pid "store-${store_id}" "$store_dir/server.log" \
    "$ROOT_DIR/scripts/ops/serve-store.sh" "${serve_args[@]}"
  STORE_PIDS+=("$store_pid")
done

echo "Cluster running. Coordinator available at ${COORDINATOR_LISTEN}"
echo "Metadata root gRPC endpoints: ${ROOT_GRPC_ADDRS[*]}"
echo "Logs are streaming to this terminal and saved under ${WORKDIR}"
wait
