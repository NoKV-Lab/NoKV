#!/usr/bin/env bash
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "the FoundationDB commit-unknown shim is qualified only on Linux" >&2
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${script_dir}/../.." && pwd)"
output_dir="${1:-${repository_root}/target/fdb-commit-unknown-shim}"
compiler="${CC:-cc}"

mkdir -p "${output_dir}"
output_dir="$(cd "${output_dir}" && pwd)"
fixture_dir="$(mktemp -d "${TMPDIR:-/tmp}/nokv-fdb-unknown-fixture.XXXXXX")"

cleanup_fixture() {
    if [[ -n "${fixture_dir}" && "${fixture_dir}" == */nokv-fdb-unknown-fixture.* ]]; then
        rm -rf -- "${fixture_dir}"
    fi
}
trap cleanup_fixture EXIT

common_flags=(-std=c11 -O2 -Wall -Wextra -Werror -Wpedantic)

"${compiler}" "${common_flags[@]}" -fPIC -shared \
    -I"${script_dir}" \
    "${script_dir}/fake_fdb_c.c" \
    -o "${output_dir}/libfake_fdb_c.so"

"${compiler}" "${common_flags[@]}" -fPIC -shared \
    "${script_dir}/fdb_commit_unknown.c" \
    -ldl -pthread \
    -o "${output_dir}/libnokv_fdb_commit_unknown.so"

"${compiler}" "${common_flags[@]}" \
    -I"${script_dir}" \
    "${script_dir}/fdb_commit_unknown_fixture.c" \
    -L"${output_dir}" -lfake_fdb_c -pthread \
    -Wl,-rpath,'$ORIGIN' \
    -o "${output_dir}/fdb_commit_unknown_fixture"

shim="${output_dir}/libnokv_fdb_commit_unknown.so"
fixture="${output_dir}/fdb_commit_unknown_fixture"
target_key_hex="746172676574"

assert_event() {
    local event_file="$1"
    local expected="$2"
    if ! grep -Fq -- "${expected}" "${event_file}"; then
        echo "missing event fragment ${expected@Q} in ${event_file}" >&2
        sed -n '1,120p' "${event_file}" >&2
        exit 1
    fi
}

run_enabled() {
    local case_name="$1"
    local scenario="$2"
    local mutation="$3"
    local mode="$4"
    local event_file="${fixture_dir}/${case_name}.jsonl"
    local nonce="fixture_${case_name}"
    : >"${event_file}"
    if [[ "${mode}" == "ordinal" ]]; then
        (
            exec 9>>"${event_file}"
            env -i \
                PATH="${PATH}" \
                LD_PRELOAD="${shim}" \
                FIXTURE_MUTATION="${mutation}" \
                NOKV_FDB_UNKNOWN_V1=1 \
                NOKV_FDB_UNKNOWN_RUN_NONCE="${nonce}" \
                NOKV_FDB_UNKNOWN_TARGET_KEY_HEX="${target_key_hex}" \
                NOKV_FDB_UNKNOWN_MUTATION="${mutation}" \
                NOKV_FDB_UNKNOWN_MODE=ordinal \
                NOKV_FDB_UNKNOWN_ORDINAL=1 \
                NOKV_FDB_UNKNOWN_EVENT_FD=9 \
                "${fixture}" "${scenario}"
        )
    else
        (
            exec 9>>"${event_file}"
            env -i \
                PATH="${PATH}" \
                LD_PRELOAD="${shim}" \
                FIXTURE_MUTATION="${mutation}" \
                NOKV_FDB_UNKNOWN_V1=1 \
                NOKV_FDB_UNKNOWN_RUN_NONCE="${nonce}" \
                NOKV_FDB_UNKNOWN_TARGET_KEY_HEX="${target_key_hex}" \
                NOKV_FDB_UNKNOWN_MUTATION="${mutation}" \
                NOKV_FDB_UNKNOWN_MODE=armed \
                NOKV_FDB_UNKNOWN_EVENT_FD=9 \
                "${fixture}" "${scenario}"
        )
    fi
    assert_event "${event_file}" '"event":"summary"'
    printf '%s\n' "${event_file}"
}

env -i PATH="${PATH}" LD_PRELOAD="${shim}" FIXTURE_MUTATION=set \
    "${fixture}" transparent

malformed_events="${fixture_dir}/malformed.jsonl"
: >"${malformed_events}"
(
    exec 9>>"${malformed_events}"
    env -i \
        PATH="${PATH}" \
        LD_PRELOAD="${shim}" \
        FIXTURE_MUTATION=set \
        NOKV_FDB_UNKNOWN_V1=1 \
        NOKV_FDB_UNKNOWN_RUN_NONCE=fixture_malformed \
        NOKV_FDB_UNKNOWN_TARGET_KEY_HEX=7461726765740 \
        NOKV_FDB_UNKNOWN_MUTATION=set \
        NOKV_FDB_UNKNOWN_MODE=ordinal \
        NOKV_FDB_UNKNOWN_ORDINAL=1 \
        NOKV_FDB_UNKNOWN_EVENT_FD=9 \
        "${fixture}" transparent
)
assert_event "${malformed_events}" '"event":"summary"'
assert_event "${malformed_events}" '"substitutions":0'
assert_event "${malformed_events}" '"invalid":true'

for mutation in set clear clear_range atomic; do
    event_file="$(run_enabled "single_${mutation}" single "${mutation}" ordinal)"
    assert_event "${event_file}" '"event":"substitution"'
    assert_event "${event_file}" '"matching_mutations":1'
    assert_event "${event_file}" '"substitutions":1'
    assert_event "${event_file}" '"invalid":false'
done

event_file="$(run_enabled nonmatch nonmatch set ordinal)"
assert_event "${event_file}" '"matching_mutations":0'
assert_event "${event_file}" '"substitutions":0'
assert_event "${event_file}" '"invalid":true'

event_file="$(run_enabled real_error real-error set ordinal)"
assert_event "${event_file}" '"event":"real_error_passthrough"'
assert_event "${event_file}" '"real_result":1031'
assert_event "${event_file}" '"substitutions":0'
assert_event "${event_file}" '"invalid":true'

event_file="$(run_enabled duplicate duplicate set ordinal)"
assert_event "${event_file}" '"event":"substitution"'
assert_event "${event_file}" '"duplicate_matches":1'
assert_event "${event_file}" '"invalid":true'

event_file="$(run_enabled destroy destroy set ordinal)"
assert_event "${event_file}" '"event":"destroyed_before_observation"'
assert_event "${event_file}" '"invalid":true'

event_file="$(run_enabled armed armed set armed)"
assert_event "${event_file}" '"event":"substitution"'
assert_event "${event_file}" '"matching_mutations":2'
assert_event "${event_file}" '"prearm_matches":1'
assert_event "${event_file}" '"arm_messages":1'
assert_event "${event_file}" '"invalid":false'

event_file="$(run_enabled duplicate_arm duplicate-arm set armed)"
assert_event "${event_file}" '"event":"substitution"'
assert_event "${event_file}" '"arm_messages":2'
assert_event "${event_file}" '"invalid":true'

event_file="$(run_enabled threaded threaded set ordinal)"
assert_event "${event_file}" '"event":"substitution"'
assert_event "${event_file}" '"selected_transactions":1'
assert_event "${event_file}" '"target_commits":1'
assert_event "${event_file}" '"invalid":false'

sha256sum "${shim}" "${fixture}" "${output_dir}/libfake_fdb_c.so"
echo "FoundationDB commit-unknown shim contract fixture: PASS"
