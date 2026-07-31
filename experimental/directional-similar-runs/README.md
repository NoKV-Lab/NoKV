# Experimental directional similar-run search

**Experimental, opt-in, and offline only.** This directory is a disposable
analysis helper. It is not part of NoKV's storage, namespace, snapshot/restore,
agent-tool, or default-search paths, and it does not build or persist an index.
It reads caller-provided JSON records into an in-memory index and prints derived
results without rewriting the source records.

## Why this location and language

The frozen NoKV repository is a Rust workspace with no existing `examples/` or
`experimental/` tree. A standalone directory using only Python's standard
library is the smallest honest placement for an opt-in analysis prototype: the
repository already contains small Python analysis tools under `scripts/`, while
this directory remains outside the workspace crates and their default build,
storage, and runtime behavior. No database, daemon, LLM call, or new dependency
is needed.

## Record contract

Input is a JSON array. Every record contains:

- `run_id`: a non-empty stable string, unique within the input;
- `features`: non-empty unique feature names in a meaningful, ordered list;
- `before` and `after`: finite numeric vectors with the same dimension as each
  other and as `features`;
- `provenance`: an object with a non-empty `source` reference. Extra provenance
  fields are preserved in output.

An optional `outcome` field is accepted as metadata, but it is deliberately not
used in delta, magnitude, direction, cosine, ranking, or the output's identity.
Changing only an outcome label therefore cannot change a fingerprint or ranking.
All records must use exactly the same feature names in exactly the same order.

For each record the tool computes `delta = after - before`, keeps its Euclidean
`magnitude` separate, and normalizes a nonzero delta into a unit `direction`.
Zero deltas are explicit (`magnitude: 0.0`, `direction: null`): they are not
compared, because cosine similarity is undefined for a zero vector. A zero
query returns no ranked results. Search excludes the query itself by default;
`--include-self` is an explicit diagnostic override. Results sort by descending
cosine similarity, then ascending `run_id`, so ties do not depend on input order.

## Usage

From this directory:

```bash
python3 directional_similar_runs.py \
  --records examples/runs.json \
  --query-run-id toy-east \
  --top-k 3
```

The JSON output includes the query fingerprint and each candidate's cosine,
delta, magnitude, normalized direction, feature order, and copied provenance.
The example labels and vectors are synthetic illustrations, not NoKV workload
results or a claim about a Transformer mechanism.

As a library in another offline analysis script:

```python
from directional_similar_runs import DirectionalIndex, load_records

index = DirectionalIndex(load_records("examples/runs.json"))
results = index.search("toy-east", top_k=3)  # self excluded by default
```

Run the deterministic focused suite:

```bash
python3 -m unittest discover -s tests -v
```

## Boundary and next validation

This helper was inspired by a small experiment by Runyuan in which reproducible
directional/low-dimensional structure appeared in one task-specific setting.
It does **not** establish a NoKV benefit, a general Transformer
mechanism, outcome/damage prediction, Kakeya causation, or a multiscale-specific
advantage. Real NoKV workload validation with frozen source records, held-out
runs, provenance, baselines, and workload-level correctness/cost measures is
required before any core/index integration could be considered. Until then,
this remains a local prototype and disposable derived artifact, not canonical
history or an authoritative result store.
