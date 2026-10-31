#!/usr/bin/env python3
"""Measure what an owner-mediated upload costs the shard's other requests.

The owner writes every artifact block to the provider itself, so a large
upload competes with the interactive requests of every other Agent on the
same shard. This probe keeps a set of long-lived `workbench-session`
processes alternating `workbench_stat` and a 1 KiB `workbench_put_file`
for a fixed wall time, and compares their latency with the shard idle
against the shard carrying one uploader that publishes 16 MiB bodies: paced
at a fixed bandwidth, then as fast as it can. Sessions, not one process per
call, so process start does not dominate the numbers. The four conditions
run as a cycle, and the cycle repeats, so a stall that happens to fall into
one window (store maintenance, the machine doing something else) shows up in
that cycle's timeline instead of passing for the uploader's cost.

The stack is the TLS gate's: Agents read the provider through its read-only
proxy and the owner holds the only writing credentials. One moto server is
started per arm; `--baseline-bin` adds a second binary measured the same way
for side-by-side evidence.

The verdict is about the candidate alone, over the samples of all cycles:
with the unpaced uploader running, the sessions' `stat` and `put` p50 may
each exceed their idle p50 by at most `--max-degradation` (a fraction,
default 0.20), and no session request or upload may fail. Interference that
the uploader's bandwidth share alone explains is accepted; a shard that
stalls every other request while a block is in flight is not.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cli_contract_gate as gate  # noqa: E402
import rpc_tls_gate as tls_gate  # noqa: E402

MIB = 1024 * 1024
UPLOAD_BODY_BYTES = 16 * MIB
CONDITIONS = (("idle", None), ("paced", "paced"), ("unpaced", "unpaced"), ("idle_again", None))
TIMELINE_BUCKET_SECONDS = 5.0


def summary(samples: list[float]) -> dict:
    if not samples:
        return {"n": 0}
    ordered = sorted(samples)

    def pick(quantile: float) -> float:
        return round(ordered[min(len(ordered) - 1, int(quantile * (len(ordered) - 1)))], 2)

    return {"n": len(ordered), "p50_ms": round(statistics.median(ordered), 2), "p90_ms": pick(0.9),
            "p99_ms": pick(0.99), "max_ms": round(ordered[-1], 2)}


def timeline(samples: dict, seconds: float) -> list[dict]:
    """Requests and p50 per bucket of wall time, to tell a stall from a load."""
    buckets = []
    start = 0.0
    while start < seconds:
        end = min(start + TIMELINE_BUCKET_SECONDS, seconds)
        window = {tool: [ms for (at, ms) in samples[tool] if start <= at < end]
                  for tool in ("workbench_stat", "workbench_put_file")}
        buckets.append({"from_s": start, "to_s": end,
                        "requests": len(window["workbench_stat"]) + len(window["workbench_put_file"]),
                        "stat_p50_ms": summary(window["workbench_stat"]).get("p50_ms"),
                        "put_p50_ms": summary(window["workbench_put_file"]).get("p50_ms"),
                        "uploads_finished": len([1 for (at, _) in samples["upload"] if start <= at < end])})
        start = end
    return buckets


def start_stack(binary: Path, args: argparse.Namespace, evidence: Path) -> tls_gate.TlsStack:
    stack_args = argparse.Namespace(nokv_bin=binary, object_bucket=args.object_bucket, object_endpoint=None,
                                    moto_server=args.moto_server, object_root=None, timeout=args.timeout)
    stack = tls_gate.TlsStack(stack_args, evidence)
    stack.provision()
    stack.identity("tenant", "tenant", "create", stack.tenant)
    stack.identity("principal", "principal", "create", stack.tenant, stack.principal)
    stack.register("agent")
    stack.identity("root", "root", "bind", stack.ids["root"], stack.tenant)
    stack.role("grant", "agent", "writer", ["--tenant-wide"])
    stack.start_owner()
    stack.workbench("probe")
    stack.success(stack.run("seed", ["workbench", "workbench_put_file", json.dumps(
        {"id": "probe", "section": "outputs", "path": "seed.txt", "text": "seed", "replace": False})]))
    return stack


def session_loop(stack: tls_gate.TlsStack, label: str, started_at: float, stop: threading.Event,
                 samples: dict) -> None:
    prefix = [*stack.route, "--agent-id", stack.ids["agent"], *stack.agent_objects,
              *stack.tls_options("agent", tls_gate.SERVER_NAME)]
    process = subprocess.Popen([str(stack.binary), *prefix, "workbench-session"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, text=True, env=stack.env)
    index = 0
    text = "x" * 1024
    try:
        while not stop.is_set():
            for tool, arguments in (
                ("workbench_stat", {"id": "probe", "section": "outputs", "path": "seed.txt"}),
                ("workbench_put_file", {"id": "probe", "section": "outputs", "path": f"{label}-{index}.txt",
                                        "text": text, "replace": False}),
            ):
                line = json.dumps({"id": str(index), "tool": tool, "arguments": arguments}) + "\n"
                started = time.perf_counter()
                process.stdin.write(line)
                process.stdin.flush()
                response = json.loads(process.stdout.readline())
                samples[tool].append((started - started_at, (time.perf_counter() - started) * 1000))
                if response.get("error") is not None:
                    samples["errors"] += 1
                index += 1
    finally:
        process.stdin.close()
        process.wait(timeout=30)


def upload_loop(stack: tls_gate.TlsStack, label: str, body: Path, started_at: float, stop: threading.Event,
                samples: dict, paced_mib_per_second: float | None) -> None:
    index = 0
    while not stop.is_set():
        started = time.perf_counter()
        call = stack.run(f"upload-{label}-{index}", [
            "--max-artifact-bytes", str(UPLOAD_BODY_BYTES + 1), "workspace-path", "put", "probe", "outputs",
            f"{label}-big-{index}.bin", "--operation-id", gate.new_id(), "--file", str(body)])
        elapsed = time.perf_counter() - started
        samples["upload"].append((time.perf_counter() - started_at, elapsed * 1000))
        if call.exit != 0:
            samples["upload_failures"] += 1
        index += 1
        if paced_mib_per_second is not None:
            budget = UPLOAD_BODY_BYTES / MIB / paced_mib_per_second
            if elapsed < budget and not stop.is_set():
                time.sleep(budget - elapsed)


def condition(stack: tls_gate.TlsStack, label: str, uploader: str | None, args: argparse.Namespace,
              body: Path) -> dict:
    samples = {"workbench_stat": [], "workbench_put_file": [], "upload": [], "errors": 0, "upload_failures": 0}
    stop = threading.Event()
    started_at = time.perf_counter()
    threads = [threading.Thread(target=session_loop, args=(stack, f"{label}-{index}", started_at, stop, samples))
               for index in range(args.sessions)]
    if uploader is not None:
        paced = args.paced_mib_per_second if uploader == "paced" else None
        threads.append(threading.Thread(target=upload_loop,
                                        args=(stack, label, body, started_at, stop, samples, paced)))
    for thread in threads:
        thread.start()
    time.sleep(args.seconds)
    stop.set()
    for thread in threads:
        thread.join()
    samples["timeline"] = timeline(samples, args.seconds)
    return samples


def result(samples: dict, seconds: float) -> dict:
    stat = [ms for (_, ms) in samples["workbench_stat"]]
    put = [ms for (_, ms) in samples["workbench_put_file"]]
    uploads = [ms for (_, ms) in samples["upload"]]
    return {"stat": summary(stat), "put": summary(put),
            "session_requests_per_second": round((len(stat) + len(put)) / seconds, 1),
            "errors": samples["errors"], "upload": summary(uploads), "upload_failures": samples["upload_failures"],
            "upload_mib_per_second": round(len(uploads) * UPLOAD_BODY_BYTES / MIB / seconds, 1)}


def measure(name: str, binary: Path, args: argparse.Namespace, evidence: Path, body: Path) -> dict:
    stack = start_stack(binary, args, evidence / name)
    cycles = []
    pooled = {label: {"workbench_stat": [], "workbench_put_file": [], "upload": [], "errors": 0,
                      "upload_failures": 0} for label, _ in CONDITIONS}
    try:
        for cycle in range(args.cycles):
            results = {}
            for label, uploader in CONDITIONS:
                samples = condition(stack, f"c{cycle}-{label}", uploader, args, body)
                results[label] = {**result(samples, args.seconds), "timeline": samples["timeline"]}
                for key in ("workbench_stat", "workbench_put_file", "upload"):
                    pooled[label][key].extend(samples[key])
                for key in ("errors", "upload_failures"):
                    pooled[label][key] += samples[key]
                print(name, f"cycle {cycle}", label,
                      json.dumps({k: v for k, v in results[label].items() if k != "timeline"}), flush=True)
            cycles.append(results)
    finally:
        stack.close()
        # The owner's metadata store grows by hundreds of MiB per condition;
        # the evidence keeps the transcripts and the report, not the store.
        shutil.rmtree(stack.metadata, ignore_errors=True)
    aggregate = {label: result(pooled[label], args.seconds * args.cycles) for label, _ in CONDITIONS}
    print(name, "aggregate", json.dumps(aggregate), flush=True)
    return {"cycles": cycles, "aggregate": aggregate}


def verdict(aggregate: dict, max_degradation: float) -> list[str]:
    failures = []
    idle, loaded = aggregate["idle"], aggregate["unpaced"]
    for tool in ("stat", "put"):
        allowed = idle[tool]["p50_ms"] * (1 + max_degradation)
        if loaded[tool]["p50_ms"] > allowed:
            failures.append(f"{tool} p50 {loaded[tool]['p50_ms']} ms under the unpaced uploader exceeds "
                            f"{allowed:.2f} ms (idle {idle[tool]['p50_ms']} ms + {max_degradation:.0%})")
    for label, measured in aggregate.items():
        if measured["errors"]:
            failures.append(f"{measured['errors']} session requests failed during {label}")
        if measured["upload_failures"]:
            failures.append(f"{measured['upload_failures']} uploads failed during {label}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nokv-bin", required=True, type=Path, help="the candidate binary the verdict is about")
    parser.add_argument("--baseline-bin", type=Path, help="a second binary measured the same way, for comparison")
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--moto-server", required=True, help="the moto_server executable to start")
    parser.add_argument("--object-bucket", default="owner-upload-probe")
    parser.add_argument("--sessions", type=int, default=8, help="interactive sessions beside the uploader")
    parser.add_argument("--seconds", type=float, default=40, help="wall time per condition")
    parser.add_argument("--cycles", type=int, default=2, help="how often the four conditions repeat")
    parser.add_argument("--paced-mib-per-second", type=float, default=20.0)
    parser.add_argument("--max-degradation", type=float, default=0.20)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    evidence = args.evidence_dir
    evidence.mkdir(parents=True, exist_ok=False)
    pattern = hashlib.sha256(b"owner-upload-interference").digest()
    body = evidence / "upload-body.bin"
    body.write_bytes((pattern * (UPLOAD_BODY_BYTES // len(pattern) + 1))[:UPLOAD_BODY_BYTES])
    report = {"sessions": args.sessions, "seconds_per_condition": args.seconds, "cycles": args.cycles,
              "paced_mib_per_second": args.paced_mib_per_second, "max_degradation": args.max_degradation,
              "arms": {}}
    failures = ["the candidate was not measured"]
    try:
        if args.baseline_bin is not None:
            report["arms"]["baseline"] = measure("baseline", args.baseline_bin, args, evidence, body)
        report["arms"]["candidate"] = measure("candidate", args.nokv_bin, args, evidence, body)
        failures = verdict(report["arms"]["candidate"]["aggregate"], args.max_degradation)
        report["verdict"] = {"qualified": not failures, "failures": failures}
    finally:
        (evidence / "report.json").write_text(json.dumps(report, indent=2))
    if failures:
        print("NOT QUALIFIED:", *failures, sep="\n  ", file=sys.stderr)
        return 1
    print("QUALIFIED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
