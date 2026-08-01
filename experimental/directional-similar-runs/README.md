# Experimental directional similar-run search

**Experimental, opt-in, and offline only.** This directory is a disposable
analysis helper. It is not part of NoKV storage, metadata, Workbench tools, or
default search, and it does not build or persist an index. It uses only the
Python standard library and never rewrites the source dataset.

## Versioned dataset contract

The input is one `directional-similar-runs/v1` object:

```json
{
  "schema": "directional-similar-runs/v1",
  "feature_space": {
    "id": "latency-profile-v1",
    "features": [
      {"name": "latency", "scale": 0.25, "unit": "seconds"},
      {"name": "tokens", "scale": 1000, "unit": "count"}
    ]
  },
  "runs": [
    {
      "run_id": "run-1",
      "before": [1.0, 1000],
      "after": [1.5, 3000],
      "provenance": {"source": "frozen/runs.json", "reference": "run-1"},
      "outcome": {"label": "optional-and-not-indexed"}
    }
  ]
}
```

- `feature_space.id` is a non-empty caller-owned label. Every fingerprint and
  result also carries the full descriptor plus a recomputed, domain-separated
  SHA-256 commitment over the ID and ordered `(name, float.hex(scale), unit)`
  definitions, so the same label cannot hide incompatible scales.
- Each feature has a unique non-empty `name`, a strictly positive finite
  `scale`, and a descriptive non-empty `unit`.
- Each run has a unique stable `run_id`, finite `before` and `after` vectors
  matching the feature-space dimension, and provenance with a non-empty
  `source`. Nested provenance is preserved in output.
- Optional `outcome` data is checked for JSON compatibility, then discarded;
  it cannot affect a fingerprint or ranking.
- Identity text must be valid UTF-8 with no leading or trailing whitespace.
  Unknown schema fields and nested metadata deeper than 64 levels are rejected.

This is intentionally a breaking contract. Legacy bare arrays and missing
scales are rejected instead of silently assuming `scale = 1.0`.

The feature-space commitment has a frozen binary preimage. It starts with the
ASCII domain `directional-similar-runs.feature-space.v1` plus one NUL byte,
then encodes the feature-space ID as `u64` big-endian byte length plus UTF-8,
the feature count as `u64` big-endian, and each ordered feature's name,
`float.hex(scale)`, and unit with the same length-prefixed UTF-8 encoding. The
published value is `sha256:` plus the lowercase digest. Changing this encoding
requires a new schema and commitment domain.

## Direction and ranking

For each feature `i`, the tool computes:

```text
raw_delta[i]    = after[i] - before[i]
scaled_delta[i] = raw_delta[i] / scale[i]
direction       = scaled_delta / hypot(scaled_delta)
```

Changing a raw unit together with its scale therefore preserves the direction
and cosine ranking. `unit` documents the raw values but does not enter the
calculation. Non-finite raw deltas, scaled deltas, or magnitudes are rejected.
If scaling would underflow a nonzero raw component to `0.0`, the record is
rejected instead of being silently treated as a zero direction.

A zero scaled delta is explicit (`scaled_magnitude: 0.0`, `direction: null`)
and is not ranked because cosine similarity is undefined. Search excludes the
query by default, skips zero-direction candidates, and orders ties by ascending
`run_id` after descending cosine similarity.

The index recursively copies and freezes nested provenance and does not expose
its internal runs. `fingerprint()` and `search()` return new disposable
dict/list trees on every call, so caller or source-record mutation cannot alter
later results.

## Usage

From this directory:

```bash
python3 directional_similar_runs.py \
  --dataset examples/runs.json \
  --query-run-id toy-east \
  --top-k 3
```

Output uses `directional-similar-runs/result/v1` and includes
the exact `feature_space`, its `feature_space_commitment`, `raw_delta`,
`scaled_delta`, `scaled_magnitude`, normalized direction, cosine, and copied
provenance.

As a library:

```python
from directional_similar_runs import DirectionalIndex, load_dataset

index = DirectionalIndex(load_dataset("examples/runs.json"))
fingerprint = index.fingerprint("toy-east")
results = index.search("toy-east", top_k=3)
```

The supported public Python symbols are `DirectionalIndex`,
`DirectionalSearchError`, `INPUT_SCHEMA`, `RESULT_SCHEMA`, and `load_dataset`.
This cutover removes the previous `load_records`, `RunVector`, `SimilarRun`,
`.runs`, and `.get()` surfaces; no compatibility alias is retained. Callers use
`fingerprint(run_id)` when they need one validated run's derived values.

Run the focused suite from the repository root:

```bash
make experimental-test
```

## Boundary and next validation

This helper was inspired by a small experiment by Runyuan in which reproducible
directional or low-dimensional structure appeared in one task-specific
setting. It does **not** establish a NoKV benefit, a general Transformer
mechanism, outcome prediction, Kakeya causation, or a multiscale-specific
advantage. Any future NoKV integration requires frozen real workloads,
held-out runs, provenance, baselines, and workload-level correctness and cost
evidence. Until then this remains a disposable offline analysis prototype.
