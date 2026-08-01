# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in, offline directional similar-run search.

This module deliberately has no NoKV storage or runtime integration. It builds
an immutable in-memory index from one versioned feature space and returns fresh,
disposable JSON-compatible result dictionaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from numbers import Real
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence


INPUT_SCHEMA = "directional-similar-runs/v1"
RESULT_SCHEMA = "directional-similar-runs/result/v1"
_MAX_METADATA_DEPTH = 64

__all__ = [
    "DirectionalIndex",
    "DirectionalSearchError",
    "INPUT_SCHEMA",
    "RESULT_SCHEMA",
    "load_dataset",
]


class DirectionalSearchError(ValueError):
    """Raised when a dataset or search request violates the contract."""


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DirectionalSearchError(f"{field} must be a non-empty string")
    if value != value.strip():
        raise DirectionalSearchError(
            f"{field} must not contain leading or trailing whitespace"
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise DirectionalSearchError(f"{field} must be valid UTF-8 text") from exc
    return value


def _sequence(value: Any, field: str) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)) or not value:
        raise DirectionalSearchError(f"{field} must be a non-empty ordered list")
    return value


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise DirectionalSearchError(f"{field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DirectionalSearchError(f"{field} must be finite") from exc
    if not math.isfinite(number):
        raise DirectionalSearchError(f"{field} must be finite")
    return number


def _finite_vector(value: Any, field: str, record_number: int) -> tuple[float, ...]:
    vector = _sequence(value, f"record {record_number}: {field}")
    return tuple(
        _finite_number(item, f"record {record_number}: {field}[{position}]")
        for position, item in enumerate(vector)
    )


def _reject_unknown_keys(value: Mapping[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(
        repr(key) for key in value if not isinstance(key, str) or key not in allowed
    )
    if unknown:
        raise DirectionalSearchError(f"{field} contains unknown fields: {unknown!r}")


def _freeze_json(
    value: Any,
    field: str,
    active: Optional[set[int]] = None,
    depth: int = 0,
) -> Any:
    """Recursively copy and freeze one JSON-compatible value."""

    if active is None:
        active = set()
    if depth > _MAX_METADATA_DEPTH:
        raise DirectionalSearchError(
            f"{field} exceeds the maximum JSON depth of {_MAX_METADATA_DEPTH}"
        )
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DirectionalSearchError(f"{field} must contain only finite JSON numbers")
        return value
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in active:
            raise DirectionalSearchError(f"{field} must be acyclic JSON")
        active.add(identity)
        try:
            frozen: dict[str, Any] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise DirectionalSearchError(f"{field} object keys must be strings")
                frozen[key] = _freeze_json(item, f"{field}.{key}", active, depth + 1)
            return MappingProxyType(frozen)
        finally:
            active.remove(identity)
    if isinstance(value, (list, tuple)):
        identity = id(value)
        if identity in active:
            raise DirectionalSearchError(f"{field} must be acyclic JSON")
        active.add(identity)
        try:
            return tuple(
                _freeze_json(item, f"{field}[{position}]", active, depth + 1)
                for position, item in enumerate(value)
            )
        finally:
            active.remove(identity)
    raise DirectionalSearchError(f"{field} must be JSON-compatible")


def _thaw_json(value: Any) -> Any:
    """Return a new JSON-compatible mutable value from frozen internal state."""

    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True)
class _FeatureSpec:
    name: str
    scale: float
    unit: str


@dataclass(frozen=True)
class _RunVector:
    run_id: str
    raw_delta: tuple[float, ...]
    scaled_delta: tuple[float, ...]
    scaled_magnitude: float
    direction: Optional[tuple[float, ...]]
    provenance: Mapping[str, Any]


def _hash_text(hasher: Any, value: str) -> None:
    encoded = value.encode("utf-8")
    hasher.update(len(encoded).to_bytes(8, "big"))
    hasher.update(encoded)


def _feature_space_commitment(
    feature_space_id: str, features: tuple[_FeatureSpec, ...]
) -> str:
    hasher = hashlib.sha256()
    hasher.update(b"directional-similar-runs.feature-space.v1\0")
    _hash_text(hasher, feature_space_id)
    hasher.update(len(features).to_bytes(8, "big"))
    for feature in features:
        _hash_text(hasher, feature.name)
        _hash_text(hasher, feature.scale.hex())
        _hash_text(hasher, feature.unit)
    return f"sha256:{hasher.hexdigest()}"


def _feature_space(value: Any) -> tuple[str, tuple[_FeatureSpec, ...]]:
    if not isinstance(value, Mapping):
        raise DirectionalSearchError("feature_space must be an object")
    _reject_unknown_keys(value, {"id", "features"}, "feature_space")
    feature_space_id = _required_text(value.get("id"), "feature_space.id")
    raw_features = _sequence(value.get("features"), "feature_space.features")

    features: list[_FeatureSpec] = []
    names: set[str] = set()
    for position, raw_feature in enumerate(raw_features):
        field = f"feature_space.features[{position}]"
        if not isinstance(raw_feature, Mapping):
            raise DirectionalSearchError(f"{field} must be an object")
        _reject_unknown_keys(raw_feature, {"name", "scale", "unit"}, field)
        name = _required_text(raw_feature.get("name"), f"{field}.name")
        if name in names:
            raise DirectionalSearchError(f"duplicate feature name: {name}")
        names.add(name)
        scale = _finite_number(raw_feature.get("scale"), f"{field}.scale")
        if scale <= 0.0:
            raise DirectionalSearchError(f"{field}.scale must be greater than zero")
        unit = _required_text(raw_feature.get("unit"), f"{field}.unit")
        features.append(_FeatureSpec(name=name, scale=scale, unit=unit))
    return feature_space_id, tuple(features)


def _provenance(value: Any, record_number: int) -> Mapping[str, Any]:
    field = f"record {record_number}: provenance"
    if not isinstance(value, Mapping):
        raise DirectionalSearchError(f"{field} must be an object with a source")
    _required_text(value.get("source"), f"{field}.source")
    frozen = _freeze_json(value, field)
    if not isinstance(frozen, Mapping):
        raise AssertionError("mapping provenance freezes to a mapping")
    return frozen


class DirectionalIndex:
    """Immutable in-memory index for one explicit scaled feature space.

    ``scaled_delta[i] = (after[i] - before[i]) / scale[i]``. A nonzero
    scaled delta is L2-normalized into the direction used for cosine ranking.
    A zero scaled delta has no direction and is never ranked.
    """

    def __init__(self, dataset: Mapping[str, Any]) -> None:
        if not isinstance(dataset, Mapping):
            raise DirectionalSearchError(
                "input must be a versioned dataset object, not a legacy run array"
            )
        _reject_unknown_keys(dataset, {"schema", "feature_space", "runs"}, "input")
        schema = dataset.get("schema")
        if schema != INPUT_SCHEMA:
            raise DirectionalSearchError(
                f"unsupported schema {schema!r}; expected {INPUT_SCHEMA!r}"
            )
        feature_space_id, features = _feature_space(dataset.get("feature_space"))
        raw_runs = _sequence(dataset.get("runs"), "runs")

        runs: dict[str, _RunVector] = {}
        for record_number, record in enumerate(raw_runs, start=1):
            if not isinstance(record, Mapping):
                raise DirectionalSearchError(f"record {record_number}: must be an object")
            _reject_unknown_keys(
                record,
                {"run_id", "before", "after", "provenance", "outcome"},
                f"record {record_number}",
            )
            run_id = _required_text(record.get("run_id"), f"record {record_number}: run_id")
            if run_id in runs:
                raise DirectionalSearchError(f"duplicate run_id: {run_id}")
            before = _finite_vector(record.get("before"), "before", record_number)
            after = _finite_vector(record.get("after"), "after", record_number)
            if len(before) != len(after):
                raise DirectionalSearchError(
                    f"record {record_number}: before and after dimensions differ"
                )
            if len(features) != len(before):
                raise DirectionalSearchError(
                    f"record {record_number}: feature space and vector dimensions differ"
                )

            raw_delta = tuple(
                after_value - before_value
                for before_value, after_value in zip(before, after)
            )
            if any(not math.isfinite(value) for value in raw_delta):
                raise DirectionalSearchError(
                    f"record {record_number}: raw delta must remain finite"
                )
            scaled_delta = tuple(
                value / feature.scale for value, feature in zip(raw_delta, features)
            )
            if any(not math.isfinite(value) for value in scaled_delta):
                raise DirectionalSearchError(
                    f"record {record_number}: scaled delta must remain finite"
                )
            if any(
                raw_value != 0.0 and scaled_value == 0.0
                for raw_value, scaled_value in zip(raw_delta, scaled_delta)
            ):
                raise DirectionalSearchError(
                    f"record {record_number}: scaled delta must not underflow to zero"
                )
            scaled_magnitude = math.hypot(*scaled_delta)
            if not math.isfinite(scaled_magnitude):
                raise DirectionalSearchError(
                    f"record {record_number}: scaled magnitude must remain finite"
                )
            direction = (
                None
                if scaled_magnitude == 0.0
                else tuple(value / scaled_magnitude for value in scaled_delta)
            )
            provenance = _provenance(record.get("provenance"), record_number)
            if "outcome" in record:
                _freeze_json(record["outcome"], f"record {record_number}: outcome")

            runs[run_id] = _RunVector(
                run_id=run_id,
                raw_delta=raw_delta,
                scaled_delta=scaled_delta,
                scaled_magnitude=scaled_magnitude,
                direction=direction,
                provenance=provenance,
            )

        self._feature_space_id = feature_space_id
        self._features = features
        self._feature_space_commitment = _feature_space_commitment(
            feature_space_id, features
        )
        self._runs: Mapping[str, _RunVector] = MappingProxyType(runs)

    def _feature_space_output(self) -> dict[str, Any]:
        return {
            "id": self._feature_space_id,
            "features": [
                {
                    "name": feature.name,
                    "scale": feature.scale,
                    "unit": feature.unit,
                }
                for feature in self._features
            ],
        }

    def _get(self, run_id: str) -> _RunVector:
        run_id = _required_text(run_id, "run_id")
        try:
            return self._runs[run_id]
        except KeyError:
            raise DirectionalSearchError(f"unknown run_id: {run_id}") from None

    def fingerprint(self, run_id: str) -> dict[str, Any]:
        """Return a fresh scaled directional fingerprint for one run."""

        run = self._get(run_id)
        return {
            "feature_space": self._feature_space_output(),
            "feature_space_commitment": self._feature_space_commitment,
            "raw_delta": list(run.raw_delta),
            "scaled_delta": list(run.scaled_delta),
            "scaled_magnitude": run.scaled_magnitude,
            "direction": None if run.direction is None else list(run.direction),
        }

    def search(
        self, query_run_id: str, top_k: int, *, exclude_self: bool = True
    ) -> list[dict[str, Any]]:
        """Return fresh deterministic top-k results ranked by scaled cosine."""

        if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k < 0:
            raise DirectionalSearchError("top_k must be a non-negative integer")
        if not isinstance(exclude_self, bool):
            raise DirectionalSearchError("exclude_self must be a boolean")

        query = self._get(query_run_id)
        if top_k == 0 or query.direction is None:
            return []

        ranked: list[tuple[float, str, _RunVector]] = []
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
            similarity = max(-1.0, min(1.0, similarity))
            ranked.append((-similarity, candidate.run_id, candidate))

        ranked.sort(key=lambda item: (item[0], item[1]))
        results: list[dict[str, Any]] = []
        for negative_similarity, _, candidate in ranked[:top_k]:
            results.append(
                {
                    "run_id": candidate.run_id,
                    "cosine_similarity": -negative_similarity,
                    "feature_space": self._feature_space_output(),
                    "feature_space_commitment": self._feature_space_commitment,
                    "raw_delta": list(candidate.raw_delta),
                    "scaled_delta": list(candidate.scaled_delta),
                    "scaled_magnitude": candidate.scaled_magnitude,
                    "direction": (
                        None
                        if candidate.direction is None
                        else list(candidate.direction)
                    ),
                    "provenance": _thaw_json(candidate.provenance),
                }
            )
        return results


def load_dataset(path: str) -> Mapping[str, Any]:
    """Load one versioned JSON dataset without modifying the source file."""

    try:
        with open(path, "r", encoding="utf-8") as source:
            payload = json.load(source)
    except UnicodeError as exc:
        raise DirectionalSearchError("input must be valid UTF-8 JSON") from exc
    except RecursionError as exc:
        raise DirectionalSearchError("input JSON exceeds the parser depth limit") from exc
    if not isinstance(payload, Mapping):
        raise DirectionalSearchError(
            "input must be a versioned dataset object, not a legacy run array"
        )
    return payload


def _cli() -> int:
    parser = argparse.ArgumentParser(
        description="Experimental offline directional similar-run search"
    )
    parser.add_argument("--dataset", required=True, help="versioned JSON dataset")
    parser.add_argument("--query-run-id", required=True, help="stable query run ID")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument(
        "--include-self",
        action="store_true",
        help="include the query run in results (self-exclusion is default)",
    )
    args = parser.parse_args()

    try:
        index = DirectionalIndex(load_dataset(args.dataset))
        query_fingerprint = index.fingerprint(args.query_run_id)
        output = {
            "schema": RESULT_SCHEMA,
            "feature_space": query_fingerprint["feature_space"],
            "feature_space_commitment": query_fingerprint[
                "feature_space_commitment"
            ],
            "query_run_id": args.query_run_id,
            "query_fingerprint": query_fingerprint,
            "top_k": args.top_k,
            "exclude_self": not args.include_self,
            "results": index.search(
                args.query_run_id,
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
