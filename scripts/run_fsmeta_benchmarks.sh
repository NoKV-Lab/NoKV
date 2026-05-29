#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Host-side benchmark code validates Eunomia grant evidence returned by the
# Compose coordinators. Keep the host test process and local Compose containers
# on the same dev/test key material; production deployments override this env.
export NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY="${NOKV_EUNOMIA_GRANT_SIGNING_PRIVATE_KEY:-rM0DUr4noWKwu7NlAoX2A6FXpdUyLESmwvqNYOkeNIc=}"

mode="${NOKV_FSMETA_BENCH_MODE:-compose}"
profile="${NOKV_FSMETA_PROFILE:-median}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
coord_addr="${NOKV_FSMETA_COORDINATOR_ADDR:-127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392}"
fsmeta_addr="${NOKV_FSMETA_ADDR:-127.0.0.1:8090}"
fsmeta_metrics_addr="${NOKV_FSMETA_METRICS_ADDR:-127.0.0.1:9400}"
mount="${NOKV_FSMETA_MOUNT:-fsmeta-bench}"
wait_attempts="${NOKV_FSMETA_WAIT_ATTEMPTS:-180}"
wait_interval="${NOKV_FSMETA_WAIT_INTERVAL:-1}"
output_dir="${NOKV_FSMETA_OUTPUT_DIR:-$ROOT/benchmark/data/fsmeta/results}"
capture_profiles="${NOKV_FSMETA_CAPTURE_PROFILES:-0}"
profile_seconds="${NOKV_FSMETA_PROFILE_SECONDS:-30}"
profile_dir="${NOKV_FSMETA_PROFILE_DIR:-$ROOT/benchmark/data/fsmeta/profiles/fsmeta_${profile}_${run_id}}"
profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=127.0.0.1:9400,store1=127.0.0.1:9200,store2=127.0.0.1:9201,store3=127.0.0.1:9202,coord1=127.0.0.1:9100,coord2=127.0.0.1:9101,coord3=127.0.0.1:9102,root1=127.0.0.1:9380,root2=127.0.0.1:9381,root3=127.0.0.1:9382}"
local_pid=""
local_tools_dir=""
local_tmp_dir=""
profile_pids=()
profile_root_dir=""
compose_built=0
local_mount_key_id="${NOKV_FSMETA_LOCAL_MOUNT_KEY_ID:-2}"
local_log_dir="${NOKV_FSMETA_LOCAL_LOG_DIR:-$ROOT/benchmark/data/fsmeta/ci}"

case "$profile" in
	median)
		default_timeout=25m
		default_stabilize_seconds=20
		;;
	long)
		default_timeout=120m
		default_stabilize_seconds=45
		;;
	official)
		default_timeout=120m
		default_stabilize_seconds=45
		;;
	*)
		echo "unknown NOKV_FSMETA_PROFILE=$profile; use median, long, or official" >&2
		exit 2
		;;
esac

clients="${NOKV_FSMETA_CLIENTS:-0}"
dirs="${NOKV_FSMETA_DIRS:-0}"
files_per_dir="${NOKV_FSMETA_FILES_PER_DIR:-0}"
users="${NOKV_FSMETA_USERS:-0}"
messages_per_user="${NOKV_FSMETA_MESSAGES_PER_USER:-0}"
workspaces="${NOKV_FSMETA_WORKSPACES:-0}"
checkpoints_per_workspace="${NOKV_FSMETA_CHECKPOINTS_PER_WORKSPACE:-0}"
files_per_checkpoint="${NOKV_FSMETA_FILES_PER_CHECKPOINT:-0}"
session_ttl="${NOKV_FSMETA_SESSION_TTL:-0}"
lookup_cache_entries="${NOKV_FSMETA_LOOKUP_CACHE_ENTRIES:-4096}"
lookup_cache_ttl="${NOKV_FSMETA_LOOKUP_CACHE_TTL:-1s}"
timeout="${NOKV_FSMETA_TIMEOUT:-$default_timeout}"
stabilize_seconds="${NOKV_FSMETA_STABILIZE_SECONDS:-$default_stabilize_seconds}"
reset_between_workloads="${NOKV_FSMETA_RESET_BETWEEN_WORKLOADS:-1}"
idle_require_pending="${NOKV_FSMETA_PERAS_IDLE_REQUIRE_PENDING:-0}"

case "$output_dir" in
	/*) ;;
	*) output_dir="$ROOT/$output_dir" ;;
esac
mkdir -p "$output_dir"

case "$local_log_dir" in
	/*) ;;
	*) local_log_dir="$ROOT/$local_log_dir" ;;
esac
mkdir -p "$local_log_dir"

case "$profile_dir" in
	/*) ;;
	*) profile_dir="$ROOT/$profile_dir" ;;
esac
profile_root_dir="$profile_dir"

enabled() {
	case "$1" in
		1|true|TRUE|yes|YES) return 0 ;;
		*) return 1 ;;
	esac
}

wait_port() {
	local addr="$1"
	local host="${addr%:*}"
	local port="${addr##*:}"
	for _ in $(seq 1 "$wait_attempts"); do
		if nc -z "$host" "$port" >/dev/null 2>&1; then
			return 0
		fi
		sleep "$wait_interval"
	done
	echo "timed out waiting for $addr" >&2
	return 1
}

peras_idle_snapshot() {
	local metrics_url="http://${fsmeta_metrics_addr%%,*}/debug/vars"
	local require_pending="${1:-0}"
	python3 - "$metrics_url" "$require_pending" <<'PY'
import json
import sys
import urllib.request

url = sys.argv[1]
with urllib.request.urlopen(url, timeout=2) as response:
	data = json.load(response)

peras = data.get("nokv_fsmeta_peras")
if not isinstance(peras, dict):
	executor = data.get("nokv_fsmeta_executor", {})
	peras = executor.get("visible_committer", {})

def number(name):
	value = peras.get(name, 0)
	if isinstance(value, bool):
		return int(value)
	if isinstance(value, (int, float)):
		return int(value)
	return 0

state = {
	"pending": number("pending"),
	"install_queue_depth": number("segment_install_queue_depth"),
	"seal_queue_depth": number("segment_seal_queue_depth"),
	"flush_total": number("flush_total"),
	"segment_total": number("segment_total"),
	"seal_total": number("seal_total"),
}
print(json.dumps(state, sort_keys=True, separators=(",", ":")))
require_pending = sys.argv[2] in ("1", "true", "TRUE", "yes", "YES")
idle = state["install_queue_depth"] == 0 and state["seal_queue_depth"] == 0
if require_pending:
	idle = idle and state["pending"] == 0
if idle:
	sys.exit(0)
sys.exit(1)
PY
}

wait_fsmeta_peras_idle() {
	local timeout_seconds="${NOKV_FSMETA_PERAS_IDLE_TIMEOUT_SECONDS:-180}"
	local interval_seconds="${NOKV_FSMETA_PERAS_IDLE_INTERVAL_SECONDS:-1}"
	local stable_polls="${NOKV_FSMETA_PERAS_IDLE_STABLE_POLLS:-2}"
	local deadline=$((SECONDS + timeout_seconds))
	local last=""
	local stable=0
	local snapshot=""
	local status=0

	while (( SECONDS < deadline )); do
		if snapshot="$(peras_idle_snapshot "$idle_require_pending" 2>/dev/null)"; then
			status=0
		else
			status=$?
		fi
		if (( status == 0 )); then
			if [[ "$snapshot" == "$last" ]]; then
				stable=$((stable + 1))
			else
				stable=1
				last="$snapshot"
			fi
			if (( stable >= stable_polls )); then
				return 0
			fi
		else
			stable=0
			if [[ -n "$snapshot" ]]; then
				last="$snapshot"
			fi
		fi
		sleep "$interval_seconds"
	done
	echo "timed out waiting for fsmeta Peras idle state; last=$last" >&2
	return 1
}

run_bench() {
	local addr="$1"
	local workloads="$2"
	local output="$3"
	echo "running fsmeta workloads: $workloads -> $output"
	(
		cd benchmark
		NOKV_FSMETA_BENCH=1 go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -v -timeout "$timeout" -args \
			-fsmeta_addr "$addr" \
			-fsmeta_coordinator_addr "$coord_addr" \
			-fsmeta_mount "$mount" \
			-fsmeta_workloads "$workloads" \
			-fsmeta_scale_profile "$profile" \
			-fsmeta_clients "$clients" \
			-fsmeta_dirs "$dirs" \
			-fsmeta_files_per_dir "$files_per_dir" \
			-fsmeta_users "$users" \
			-fsmeta_messages_per_user "$messages_per_user" \
			-fsmeta_workspaces "$workspaces" \
			-fsmeta_checkpoints_per_workspace "$checkpoints_per_workspace" \
			-fsmeta_files_per_checkpoint "$files_per_checkpoint" \
			-fsmeta_session_ttl "$session_ttl" \
			-fsmeta_lookup_cache_entries "$lookup_cache_entries" \
			-fsmeta_lookup_cache_ttl "$lookup_cache_ttl" \
			-fsmeta_timeout "$timeout" \
			-fsmeta_output "$output"
	)
}

profiles_enabled() {
	enabled "$capture_profiles"
}

write_profile_manifest() {
	local workloads="$1"
	cat >"$profile_dir/manifest.txt" <<EOF
run_id=$run_id
bench_mode=$mode
benchmark_profile=$profile
profile_file=benchmark/fsmeta/profiles/official/workloads.yaml
workloads=$workloads
clients_override=$clients
dirs_override=$dirs
files_per_dir_override=$files_per_dir
users_override=$users
messages_per_user_override=$messages_per_user
workspaces_override=$workspaces
checkpoints_per_workspace_override=$checkpoints_per_workspace
files_per_checkpoint_override=$files_per_checkpoint
session_ttl_override=$session_ttl
reset_between_workloads=$reset_between_workloads
peras_idle_require_pending=$idle_require_pending
lookup_cache_entries=$lookup_cache_entries
lookup_cache_ttl=$lookup_cache_ttl
profile_seconds=$profile_seconds
targets=$profile_targets
EOF
}

write_benchmark_manifest() {
	local output="$1"
	local workloads="$2"
	cat >"${output}.manifest.txt" <<EOF
run_id=$run_id
bench_mode=$mode
scale_profile=$profile
profile_file=benchmark/fsmeta/profiles/official/workloads.yaml
workloads=$workloads
clients_override=$clients
dirs_override=$dirs
files_per_dir_override=$files_per_dir
users_override=$users
messages_per_user_override=$messages_per_user
workspaces_override=$workspaces
checkpoints_per_workspace_override=$checkpoints_per_workspace
files_per_checkpoint_override=$files_per_checkpoint
session_ttl_override=$session_ttl
reset_between_workloads=$reset_between_workloads
peras_idle_require_pending=$idle_require_pending
lookup_cache_entries=$lookup_cache_entries
lookup_cache_ttl=$lookup_cache_ttl
timeout=$timeout
note=exact resolved per-workload scales are emitted by the Go benchmark manifests next to each workload CSV
EOF
}

start_profile_capture() {
	local workloads="$1"
	if ! profiles_enabled; then
		return
	fi
	mkdir -p "$profile_dir"
	write_profile_manifest "$workloads"
	echo "capturing fsmeta profile bundle in $profile_dir"
	IFS=',' read -r -a targets <<<"$profile_targets"
	for target in "${targets[@]}"; do
		local name="${target%%=*}"
		local addr="${target#*=}"
		if [[ -z "$name" || -z "$addr" || "$name" == "$addr" ]]; then
			echo "skip malformed profile target: $target" >&2
			continue
		fi
		(
			curl -fsS --max-time "$((profile_seconds + 15))" \
				"http://$addr/debug/pprof/profile?seconds=$profile_seconds" \
				-o "$profile_dir/${name}.cpu.pprof" \
				>"$profile_dir/${name}.cpu.log" 2>&1 || \
				echo "cpu profile capture failed for $name at $addr" >>"$profile_dir/${name}.cpu.log"
		) &
		profile_pids+=("$!")
	done
}

fetch_profile_file() {
	local url="$1"
	local output="$2"
	curl -fsS --max-time 15 "$url" -o "$output" >/dev/null 2>&1 || \
		echo "profile fetch failed: $url" >"$output.error"
}

collect_profile_snapshots() {
	if ! profiles_enabled; then
		return
	fi
	IFS=',' read -r -a targets <<<"$profile_targets"
	for target in "${targets[@]}"; do
		local name="${target%%=*}"
		local addr="${target#*=}"
		if [[ -z "$name" || -z "$addr" || "$name" == "$addr" ]]; then
			continue
		fi
		fetch_profile_file "http://$addr/debug/vars" "$profile_dir/${name}.vars.json"
		fetch_profile_file "http://$addr/debug/pprof/goroutine?debug=2" "$profile_dir/${name}.goroutine.txt"
		fetch_profile_file "http://$addr/debug/pprof/heap" "$profile_dir/${name}.heap.pprof"
		fetch_profile_file "http://$addr/debug/pprof/allocs" "$profile_dir/${name}.allocs.pprof"
	done
}

finish_profile_capture() {
	if ! profiles_enabled; then
		return
	fi
	for pid in "${profile_pids[@]}"; do
		wait "$pid" || true
	done
	collect_profile_snapshots
	tar -C "$(dirname "$profile_dir")" -czf "$profile_dir.tar.gz" "$(basename "$profile_dir")"
	echo "wrote fsmeta profile bundle: $profile_dir.tar.gz"
}

print_bench_summary() {
	local output="$1"
	if [[ ! -f "$output" ]]; then
		return
	fi
	echo "fsmeta benchmark CSV summary:"
	sed -n '1,120p' "$output"
}

start_compose_cluster() {
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" != "1" ]]; then
		return
	fi
	# The benchmark mount must exist before the fsmeta gateway starts.
	# Otherwise the run depends on asynchronous root watch catch-up during
	# the benchmark bootstrap window, which makes long/profiled CI runs flaky.
	export NOKV_MOUNT_IDS="${NOKV_MOUNT_IDS:-default,$mount}"
	if [[ "${NOKV_FSMETA_COMPOSE_BUILD:-1}" == "1" && "$compose_built" == "0" ]]; then
		echo "starting Docker Compose NoKV cluster with local build"
		docker compose up -d --build
		compose_built=1
	else
		echo "starting Docker Compose NoKV cluster"
		docker compose up -d
	fi
	wait_port "${fsmeta_addr%%,*}"
	wait_port "${coord_addr%%,*}"
	if [[ "$stabilize_seconds" != "0" ]]; then
		# A listening gRPC port is not enough for a fair storage benchmark:
		# freshly started Compose clusters can still be electing Raft leaders
		# and publishing coordinator duty grants.
		echo "waiting ${stabilize_seconds}s for raft leaders and coordinator grants to settle"
		sleep "$stabilize_seconds"
	fi
}

reset_compose_cluster() {
	if [[ "${NOKV_FSMETA_COMPOSE:-1}" != "1" ]]; then
		return
	fi
	docker compose down -v
}

run_compose_benchmarks() {
	local workloads="${NOKV_FSMETA_WORKLOADS:-mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_compose_${profile}_${run_id}_isolated.csv}"
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if ! enabled "$reset_between_workloads"; then
		start_compose_cluster
		wait_fsmeta_peras_idle
		profile_dir="$profile_root_dir"
		profile_pids=()
		start_profile_capture "$workloads"
	fi
	local combined_tmp="$output.tmp"
	local wrote_header=0
	local bench_status=0
	rm -f "$combined_tmp" "$output"
	IFS=',' read -r -a workload_list <<<"$workloads"
	for workload in "${workload_list[@]}"; do
		workload="${workload//[[:space:]]/}"
		if [[ -z "$workload" ]]; then
			continue
		fi
		local safe_workload="${workload//[^A-Za-z0-9_-]/_}"
		local workload_output="$output_dir/fsmeta_compose_${profile}_${run_id}_${safe_workload}.csv"
		echo "running isolated fsmeta workload: $workload"
		if enabled "$reset_between_workloads"; then
			reset_compose_cluster
			start_compose_cluster
			wait_fsmeta_peras_idle
			profile_dir="$profile_root_dir/$safe_workload"
			profile_pids=()
			start_profile_capture "$workload"
		fi
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if enabled "$reset_between_workloads"; then
			finish_profile_capture
		fi
		if [[ "$bench_status" -ne 0 ]]; then
			break
		fi
		if [[ "$wrote_header" -eq 0 ]]; then
			cat "$workload_output" >"$combined_tmp"
			wrote_header=1
		else
			tail -n +2 "$workload_output" >>"$combined_tmp"
		fi
		print_bench_summary "$workload_output"
		if ! enabled "$reset_between_workloads"; then
			wait_fsmeta_peras_idle
		fi
	done
	if ! enabled "$reset_between_workloads"; then
		finish_profile_capture
	fi
	if [[ "$bench_status" -ne 0 ]]; then
		rm -f "$combined_tmp"
		exit "$bench_status"
	fi
	if [[ "$wrote_header" -eq 0 ]]; then
		echo "no fsmeta workloads selected" >&2
		rm -f "$combined_tmp"
		exit 2
	fi
	mv "$combined_tmp" "$output"
	write_benchmark_manifest "$output" "$workloads"
	print_bench_summary "$output"
	echo "wrote isolated fsmeta benchmark summary: $output"
	if [[ "${NOKV_FSMETA_COMPOSE_DOWN:-0}" == "1" ]]; then
		docker compose down -v
	fi
}

ensure_local_gateway_binary() {
	if [[ -n "$local_tools_dir" ]]; then
		return
	fi
	local_tools_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-local-tools.XXXXXX")"
	go build -o "$local_tools_dir/nokv-fsmeta" ./cmd/nokv-fsmeta
}

stop_local_gateway() {
	if [[ -n "$local_pid" ]]; then
		kill "$local_pid" 2>/dev/null || true
		wait "$local_pid" 2>/dev/null || true
		local_pid=""
	fi
	if [[ -z "${NOKV_FSMETA_LOCAL_WORKDIR:-}" && -n "$local_tmp_dir" ]]; then
		rm -rf "$local_tmp_dir"
		local_tmp_dir=""
	fi
}

cleanup_local_gateway() {
	stop_local_gateway
	if [[ -n "$local_tools_dir" ]]; then
		rm -rf "$local_tools_dir"
		local_tools_dir=""
	fi
}

start_local_gateway() {
	local suffix="$1"
	ensure_local_gateway_binary
	stop_local_gateway

	local work_dir
	if [[ -n "${NOKV_FSMETA_LOCAL_WORKDIR:-}" ]]; then
		work_dir="$NOKV_FSMETA_LOCAL_WORKDIR"
		if enabled "$reset_between_workloads"; then
			work_dir="$work_dir/$suffix"
			rm -rf "$work_dir"
		fi
	else
		local_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fsmeta-local.XXXXXX")"
		work_dir="$local_tmp_dir/db"
	fi
	mkdir -p "$work_dir"

	local log_file="$local_log_dir/fsmeta-local-${profile}-${run_id}-${suffix}.log"
	echo "starting local fsmeta gateway on $fsmeta_addr work_dir=$work_dir"
	"$local_tools_dir/nokv-fsmeta" \
		--backend local \
		--addr "$fsmeta_addr" \
		--metrics-addr "$fsmeta_metrics_addr" \
		--local-work-dir "$work_dir" \
		--local-mount-id "$mount" \
		--local-mount-key-id "$local_mount_key_id" \
		>"$log_file" 2>&1 &
	local_pid="$!"
	if ! wait_port "${fsmeta_addr%%,*}"; then
		cat "$log_file" >&2 || true
		return 1
	fi
	wait_port "${fsmeta_metrics_addr%%,*}"
}

run_local_benchmarks() {
	local workloads="${NOKV_FSMETA_WORKLOADS:-mdtest-easy,mdtest-hard,filebench-varmail,mimesis-namespace,ai-checkpoint-agent}"
	local output="${NOKV_FSMETA_OUTPUT:-$output_dir/fsmeta_local_${profile}_${run_id}_isolated.csv}"
	local coord_addr=""
	local profile_targets="${NOKV_FSMETA_PROFILE_TARGETS:-fsmeta=$fsmeta_metrics_addr}"
	trap cleanup_local_gateway EXIT
	case "$output" in
		/*) ;;
		*) output="$ROOT/$output" ;;
	esac
	if ! enabled "$reset_between_workloads"; then
		start_local_gateway "shared"
		profile_dir="$profile_root_dir"
		profile_pids=()
		start_profile_capture "$workloads"
	fi
	local combined_tmp="$output.tmp"
	local wrote_header=0
	local bench_status=0
	rm -f "$combined_tmp" "$output"
	IFS=',' read -r -a workload_list <<<"$workloads"
	for workload in "${workload_list[@]}"; do
		workload="${workload//[[:space:]]/}"
		if [[ -z "$workload" ]]; then
			continue
		fi
		local safe_workload="${workload//[^A-Za-z0-9_-]/_}"
		local workload_output="$output_dir/fsmeta_local_${profile}_${run_id}_${safe_workload}.csv"
		echo "running isolated local fsmeta workload: $workload"
		if enabled "$reset_between_workloads"; then
			start_local_gateway "$safe_workload"
			profile_dir="$profile_root_dir/$safe_workload"
			profile_pids=()
			start_profile_capture "$workload"
		fi
		set +e
		run_bench "$fsmeta_addr" "$workload" "$workload_output"
		bench_status=$?
		set -e
		if enabled "$reset_between_workloads"; then
			finish_profile_capture
			stop_local_gateway
		fi
		if [[ "$bench_status" -ne 0 ]]; then
			break
		fi
		if [[ "$wrote_header" -eq 0 ]]; then
			cat "$workload_output" >"$combined_tmp"
			wrote_header=1
		else
			tail -n +2 "$workload_output" >>"$combined_tmp"
		fi
		print_bench_summary "$workload_output"
	done
	if ! enabled "$reset_between_workloads"; then
		finish_profile_capture
	fi
	if [[ "$bench_status" -ne 0 ]]; then
		rm -f "$combined_tmp"
		exit "$bench_status"
	fi
	if [[ "$wrote_header" -eq 0 ]]; then
		echo "no fsmeta workloads selected" >&2
		rm -f "$combined_tmp"
		exit 2
	fi
	mv "$combined_tmp" "$output"
	write_benchmark_manifest "$output" "$workloads"
	print_bench_summary "$output"
	echo "wrote isolated local fsmeta benchmark summary: $output"
}

case "$mode" in
	compose)
		run_compose_benchmarks
		;;
	local)
		run_local_benchmarks
		;;
	*)
		echo "unknown NOKV_FSMETA_BENCH_MODE=$mode; use compose or local" >&2
		exit 2
		;;
esac
