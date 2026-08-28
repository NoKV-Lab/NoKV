#!/usr/bin/env python3
# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

"""Closed evidence publication for source-bound live qualification producers."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Mapping, Sequence

from source_bound_producer import (
    OUTCOME_EXIT_CODES,
    ProducerError,
    QualificationContext,
    load_context,
    write_create_new_evidence,
    write_producer_result,
)


QUALIFICATION_ROLE = "qualification"
TRANSCRIPT_FILES = {
    "mcp-transcript": "mcp-transcript.jsonl",
    "cli-transcript": "cli-transcript.jsonl",
}


def load_live_context(
    *,
    producer_id: str,
    scenarios: Mapping[str, object],
    dependency_names: Sequence[str],
    product_binary: Path,
    evidence_roles: Sequence[str],
    environ: Mapping[str, str] | None = None,
) -> QualificationContext:
    """Load a runner-bound live context and exact-bind its product binary argument."""

    context = load_context(
        os.environ if environ is None else environ,
        producer_id=producer_id,
        evidence_kind="live",
        scenarios=scenarios,
        require_product_binary=True,
        expected_dependencies=dependency_names,
        required_evidence_roles=evidence_roles,
    )
    subject = context.subjects["product_binary"]
    if not isinstance(subject, dict):
        raise ProducerError("runner product binary subject uses an invalid schema")
    supplied = product_binary.resolve()
    if supplied != Path(str(subject["path"])).resolve():
        raise ProducerError("product binary argument does not match the runner subject")
    return context


def publish_live_result(
    *,
    result_path: Path,
    context: QualificationContext,
    outcome: str,
    qualification: Mapping[str, object],
    evidence_roles: Sequence[str],
    transcript: bytes | None = None,
) -> None:
    """Publish direct-child evidence before the terminal structured result."""

    if outcome not in OUTCOME_EXIT_CODES:
        raise ProducerError(f"invalid live producer outcome {outcome!r}")
    roles = tuple(evidence_roles)
    if not roles or roles[0] != "producer-result" or len(roles) != len(set(roles)):
        raise ProducerError(
            "live evidence roles must be unique and start with producer-result"
        )
    transcript_roles = tuple(role for role in roles if role in TRANSCRIPT_FILES)
    if len(transcript_roles) > 1:
        raise ProducerError("a live producer may retain only one transport transcript")
    if transcript_roles and transcript is None and outcome == "PASS":
        label = "MCP" if transcript_roles[0] == "mcp-transcript" else "native CLI"
        raise ProducerError(f"a live PASS requires the real {label} transcript")
    result_path = result_path.resolve()
    if QUALIFICATION_ROLE in roles:
        payload = (
            json.dumps(qualification, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        )
        write_create_new_evidence(
            result_path.parent / "qualification.json",
            payload,
            operation_id=context.operation_id,
            label="typed live qualification evidence",
        )
    for role in transcript_roles:
        if transcript is None:
            transcript = (
                json.dumps(
                    {
                        "schema": f"nokv.pre423.{role.replace('-', '_')}_gap.v1",
                        "outcome": outcome,
                        "reason": qualification.get(
                            "reason", "live transcript unavailable"
                        ),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
                + b"\n"
            )
        write_create_new_evidence(
            result_path.parent / TRANSCRIPT_FILES[role],
            transcript,
            operation_id=context.operation_id,
            label=f"typed live {role} evidence",
        )
    write_producer_result(
        result_path,
        context,
        outcome,
        evidence_roles=roles,
    )


def gap_record(*, producer: str, reason: str) -> dict[str, object]:
    if not reason.strip():
        raise ProducerError("qualification gap reason must be non-empty")
    return {
        "schema": "nokv.pre423.live_qualification_gap.v1",
        "producer": producer,
        "status": "NOT QUALIFIED",
        "reason": reason,
    }


def gap_main(
    *,
    producer_id: str,
    scenarios: Mapping[str, object],
    dependency_names: Sequence[str],
    evidence_roles: Sequence[str],
    reason: str,
    description: str,
    argv: Sequence[str] | None = None,
) -> int:
    """Emit a runner-bound NQ result for a deliberately unimplemented live gate."""

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--qualification-result", required=True, type=Path)
    parser.add_argument("--nokv-bin", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        context = load_live_context(
            producer_id=producer_id,
            scenarios=scenarios,
            dependency_names=dependency_names,
            product_binary=args.nokv_bin,
            evidence_roles=evidence_roles,
        )
        publish_live_result(
            result_path=args.qualification_result,
            context=context,
            outcome="NQ",
            qualification=gap_record(producer=producer_id, reason=reason),
            evidence_roles=evidence_roles,
        )
    except (OSError, ProducerError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 2
    print(f"NOT QUALIFIED: {reason}", file=sys.stderr)
    return OUTCOME_EXIT_CODES["NQ"]
