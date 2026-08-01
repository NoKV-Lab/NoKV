# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in, offline directional similar-run search.

This module deliberately has no NoKV storage or runtime integration.  It builds
an in-memory index from caller-provided records and returns disposable result
dictionaries.  Outcome labels are retained on the in-memory record only as
metadata; they are never read while constructing fingerprints or scores.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
from dataclasses import dataclass
from numbers import Real
from typing import Any, Iterable, Mapping, Optional, Sequence


class DirectionalSearchError(ValueError):
    """Raised when run records or a search request violate the contract."""


_MISSING = object()


def _finite_vector(value: Any, field: str, record_number: int) -> tuple[float, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise DirectionalSearchError(
            f"record {record_number}: {field} must be a non-empty numeric vector"
        )

    result: list[float] = []
    for position, item in enumerate(value):
        if isinstance(item, bool) or not isinstance(item, Real):
            raise DirectionalSearchError(
                f"record {record_number}: {field}[{position}] must be numeric"
            )
        try:
            number = float(item)
        except (TypeError, ValueError, OverflowError) as exc:
            raise DirectionalSearchError(
                f"record {record_number}: {field}[{position}] must be finite"
            ) from exc
        if not math.isfinite(number):
            raise DirectionalSearchError(
                f"record {record_number}: {field}[{position}] must be finite"
            )
        result.append(number)
    return tuple(result)


def _feature_names(value: Any, record_number: int) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise DirectionalSearchError(
            f"record {record_number}: features must be a non-empty ordered list"
        )
    if any(not isinstance(name, str) or not name.strip() for name in value):
        raise DirectionalSearchError(
            f"record {record_number}: feature names must be non-empty strings"
        )
    result = tuple(value)
    if len(set(result)) != len(result):
        raise DirectionalSearchError(
            f"record {record_number}: feature names must be unique"
        )
    return result


def _run_id(value: Any, record_number: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DirectionalSearchError(
            f"record {record_number}: run_id must be a non-empty string"
        )
    return value


def _provenance(value: Any, record_number: int) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DirectionalSearchError(
            f"record {record_number}: provenance must be an object with a source"
        )
    source = value.get("source")
    if not isinstance(source, str) or not source.strip():
        raise DirectionalSearchError(
            f"record {record_number}: provenance.source must be a non-empty string"
        )
    return copy.deepcopy(value)


@dataclass(frozen=True)
class RunVector:
    """Validated, immutable-in-use representation of one source run."""

    run_id: str
    features: tuple[str, ...]
    before: tuple[float, ...]
    after: tuple[float, ...]
    delta: tuple[float, ...]
    magnitude: float
    direction: Optional[tuple[float, ...]]
    provenance: Mapping[str, Any]
    outcome: Any = _MISSING

    def fingerprint(self) -> Optional[tuple[float, ...]]:
        """Return the unit delta direction, or ``None`` for a zero delta."""

        return self.direction


@dataclass(frozen=True)
class SimilarRun:
    """A ranked result with source provenance and no source-record mutation."""

    run_id: str
    cosine_similarity: float
    features: tuple[str, ...]
    delta: tuple[float, ...]
    magnitude: float
    direction: Optional[tuple[float, ...]]
    provenance: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible copy of this disposable result."""

        return {
            "run_id": self.run_id,
            "cosine_similarity": self.cosine_similarity,
            "features": list(self.features),
            "delta": list(self.delta),
            "magnitude": self.magnitude,
            "direction": None if self.direction is None else list(self.direction),
            "provenance": copy.deepcopy(self.provenance),
        }


class DirectionalIndex:
    """An in-memory, rebuildable index for directional similar-run search.

    Every record must use the same feature names in the same order.  The index
    owns no source files and stores no persistent sidecar.  A zero-delta run is
    represented with magnitude ``0.0`` and ``direction=None``; it is not ranked
    because cosine similarity is undefined for a zero vector.
    """

    def __init__(self, records: Iterable[Mapping[str, Any]]) -> None:
        if isinstance(records, (str, bytes, Mapping)):
            raise DirectionalSearchError("records must be an iterable of record objects")
        try:
            materialized = list(records)
        except TypeError as exc:
            raise DirectionalSearchError(
                "records must be an iterable of record objects"
            ) from exc
        if not materialized:
            raise DirectionalSearchError("records must not be empty")

        self._runs: dict[str, RunVector] = {}
        expected_features: Optional[tuple[str, ...]] = None
        for record_number, record in enumerate(materialized, start=1):
            if not isinstance(record, Mapping):
                raise DirectionalSearchError(
                    f"record {record_number}: must be an object"
                )
            run_id = _run_id(record.get("run_id"), record_number)
            if run_id in self._runs:
                raise DirectionalSearchError(f"duplicate run_id: {run_id}")
            features = _feature_names(record.get("features"), record_number)
            before = _finite_vector(record.get("before"), "before", record_number)
            after = _finite_vector(record.get("after"), "after", record_number)
            if len(before) != len(after):
                raise DirectionalSearchError(
                    f"record {record_number}: before and after dimensions differ"
                )
            if len(features) != len(before):
                raise DirectionalSearchError(
                    f"record {record_number}: features and vector dimensions differ"
                )
            if expected_features is None:
                expected_features = features
            elif features != expected_features:
                raise DirectionalSearchError(
                    "feature semantics/order differ: "
                    f"expected {expected_features!r}, got {features!r}"
                )

            delta = tuple(
                after_value - before_value
                for before_value, after_value in zip(before, after)
            )
            if any(not math.isfinite(value) for value in delta):
                raise DirectionalSearchError(
                    f"record {record_number}: delta must remain finite"
                )
            magnitude = math.hypot(*delta)
            if not math.isfinite(magnitude):
                raise DirectionalSearchError(
                    f"record {record_number}: delta magnitude must remain finite"
                )
            direction: Optional[tuple[float, ...]]
            if magnitude == 0.0:
                direction = None
            else:
                direction = tuple(value / magnitude for value in delta)

            self._runs[run_id] = RunVector(
                run_id=run_id,
                features=features,
                before=before,
                after=after,
                delta=delta,
                magnitude=magnitude,
                direction=direction,
                provenance=_provenance(record.get("provenance"), record_number),
                outcome=copy.deepcopy(record["outcome"])
                if "outcome" in record
                else _MISSING,
            )

    @property
    def runs(self) -> tuple[RunVector, ...]:
        """Return validated runs in source iteration order."""

        return tuple(self._runs.values())

    def get(self, run_id: str) -> RunVector:
        """Return one run or raise a contract error for an unknown ID."""

        try:
            return self._runs[run_id]
        except KeyError:
            raise DirectionalSearchError(f"unknown run_id: {run_id}") from None

    def fingerprint(self, run_id: str) -> dict[str, Any]:
        """Return delta, magnitude, and normalized direction for one run."""

        run = self.get(run_id)
        return {
            "delta": list(run.delta),
            "magnitude": run.magnitude,
            "direction": None if run.direction is None else list(run.direction),
        }

    def search(
        self, query_run_id: str, top_k: int, *, exclude_self: bool = True
    ) -> list[dict[str, Any]]:
        """Return deterministic top-k nonzero runs ranked by cosine similarity.

        ``exclude_self`` defaults to true and can be set false explicitly for
        diagnostics.  A zero-direction query returns an empty list because no
        cosine ranking is defined; zero-direction candidates are skipped.
        Ties are ordered by ascending stable ``run_id``.
        """

        if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k < 0:
            raise DirectionalSearchError("top_k must be a non-negative integer")
        if not isinstance(exclude_self, bool):
            raise DirectionalSearchError("exclude_self must be a boolean")

        query = self.get(query_run_id)
        if top_k == 0 or query.direction is None:
            return []

        ranked: list[SimilarRun] = []
        for candidate in self._runs.values():
            if exclude_self and candidate.run_id == query.run_id:
                continue
            if candidate.direction is None:
                continue
            similarity = math.fsum(
                query_component * candidate_component
                for query_component, candidate_component in zip(
                    query.direction, candidate.direction
                )
            )
            # Unit-vector rounding can produce a value infinitesimally outside
            # the cosine range.  Clamp only that arithmetic artifact.
            similarity = max(-1.0, min(1.0, similarity))
            ranked.append(
                SimilarRun(
                    run_id=candidate.run_id,
                    cosine_similarity=similarity,
                    features=candidate.features,
                    delta=candidate.delta,
                    magnitude=candidate.magnitude,
                    direction=candidate.direction,
                    provenance=candidate.provenance,
                )
            )

        ranked.sort(key=lambda result: (-result.cosine_similarity, result.run_id))
        return [result.as_dict() for result in ranked[:top_k]]


def load_records(path: str) -> list[Mapping[str, Any]]:
    """Load a JSON array of run records without modifying the source file."""

    with open(path, "r", encoding="utf-8") as source:
        payload = json.load(source)
    if not isinstance(payload, list):
        raise DirectionalSearchError("input must be a JSON array of run records")
    return payload


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Experimental offline directional similar-run search"
    )
    parser.add_argument("--records", required=True, help="JSON array of run records")
    parser.add_argument("--query-run-id", required=True, help="stable query run ID")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument(
        "--include-self",
        action="store_true",
        help="include the query run in results (self-exclusion is default)",
    )
    args = parser.parse_args()

    try:
        index = DirectionalIndex(load_records(args.records))
        query = index.get(args.query_run_id)
        output = {
            "query_run_id": query.run_id,
            "query_fingerprint": index.fingerprint(query.run_id),
            "top_k": args.top_k,
            "exclude_self": not args.include_self,
            "results": index.search(
                query.run_id,
                args.top_k,
                exclude_self=not args.include_self,
            ),
        }
    except (OSError, json.JSONDecodeError, DirectionalSearchError) as exc:
        parser.error(str(exc))
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
