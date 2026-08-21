<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# pre-#423 Workbench Contract Ledger

[`pre423_contract_ledger.json`](../../scripts/workbench/pre423_contract_ledger.json)
is the machine-readable recovery backlog for the 47 behaviors inventoried at
NoKV revision `98cac201affee7ca1a654fea39373108b81d31ef`. It contains 39 core
Workbench contracts and eight legacy SDK or filesystem perimeter contracts.
It is a behavior oracle, not permission to restore the old metadata layout.
It is not an inventory or qualification claim for every pre-#423 feature, and
47/47 does not by itself qualify every phase of the wider recovery program.

This ledger preserves historical MCP and LingTai evidence classes; it does not
set current delivery priority. Current integrations use the native full CLI
first, the direct Python SDK second, and the 18-tool endpoint only as an
optional MCP sidecar. A sidecar-specific receipt qualifies only that boundary.

Each stable item records:

- class `A`, `B`, `C`, or `D` from the pre-test porting decision;
- current disposition: `restore`, `replace`, `retire`, or `do-not-restore`;
- the package or deployment owner and observable boundary;
- revision-relative source evidence; and
- the gates required before the item can be called recovered or deliberately
  retired.

Every required gate also has an item-specific qualification expectation. The
expectation names one or more stable scenarios and resolves an evidence profile
that lists the only accepted evidence kinds and producers. The ledger contains
47 items, 137 required-gate references, and 172 scenarios. Validator policy
fixes all three relationships: deleting a gate, omitting its expectation, using
a scenario from another item, or broadening a producer or evidence kind fails
before qualification runs.

Class A ports the public behavior unchanged. Class B preserves the invariant
but rewrites the test through `RootId`, path-native workspace types, immutable
artifact revisions, typed lifecycle operations, and current recovery fences.
Class C requires an explicit replacement contract. Class D is excluded from
the recovery backlog. For this recovery ledger, only `L07` FUSE/POSIX may be
excluded; the Python byte/range, Workbench-scoped fsspec, checkpoint, and torch
DCP behaviors in `L03` through `L06` must be replaced and live-tested rather
than retired. The generic seven-tool Agent MCP profile in `L01` must coexist
with the 18-tool Workbench profile, and the operational outcomes in `L08` must
be rebuilt rather than satisfied by an API-decision document.

The ledger never makes FUSE, POSIX, inode/dentry layout, a second durable
schema, or naked-offset pagination part of current product acceptance. The
fsspec replacement is deliberately narrower: it is scoped to one Workbench,
uses whole-object immutable publication, and promises no POSIX directory or
mount behavior. Where an old test contains excluded implementation details,
retain only the higher-level invariant and test it at the current owner
boundary. In particular, logical display roots do not establish isolation: the
Agent integration must persist distinct `RootId` authority.

Validate the ledger and its policy tests with:

```bash
python3 scripts/workbench/pre423_contract_ledger.py
python3 scripts/workbench/pre423_contract_ledger_test.py
```

Any recovery change should update the applicable item only when its listed
gate exists and passes at that boundary. Do not mark a schema-only check as
evidence for runtime behavior, restore composition, isolation, durability,
provider recovery, or real LingTai MCP integration.

## Evidence kinds are not interchangeable

There is no implicit ranking in which one evidence kind can satisfy another:

- `static` inspects checked-in source, package graphs, schemas, or product
  decisions without executing the product behavior;
- `unit` executes one production package with in-memory or fake collaborators;
- `integration` executes multiple production packages or a non-production
  test-support binary across an internal boundary; and
- `live` executes a shipping binary or installed SDK across the real external
  boundary named by the gate.

An expectation must explicitly allow the kind. For example, live provider
recovery cannot replace LingTai MCP evidence, and the current raw MCP harness
cannot identify itself as the `lingtai-mcp` producer. Conversely, an item does
not need live evidence when its expectation deliberately requires only a unit
or static boundary.

## Command-bound qualification receipts

`qualification_receipt.py` is the only supported receipt writer. It validates
all claims against the checked-in ledger before running anything. A caller
cannot wrap an arbitrary command: each producer is bound to one checked-in
Python entrypoint, the runner's own Python interpreter, an exact structured
result argument, forbidden arguments such as `--dry-run`, required evidence
roles, and its allowed scenarios. The runner invokes the argument vector with
`shell=False` and derives the full git SHA and dirty state from `--repo`;
callers cannot supply either value.

Every producer writes `nokv.pre423.producer_result.v1`. The result must bind the
runner-created operation id, source SHA, command argv hash, exact scenario set,
scenario outcome, evidence roles, and the complete subjects that the producer
independently observed. Echoing only a runner-provided subject hash is not
sufficient. Live producers must also bind the declared product binary to the
producer's exact `--nokv-bin` or `--binary` argument and provide the exact
dependency names required by their catalog entry. Dependency identities are
pinned `sha256:`, `git:`, or digest-qualified `oci:` identities as allowed for
that producer; free-form names or versions are rejected.

The runner rechecks checkout, entrypoint, interpreter, and product-binary
identity after the command. A command that changes any of them cannot produce
`PASS`. `--evidence-root` must be new or empty, outside the checkout, and every
declared evidence file must be a newly created direct child. After execution,
the runner rejects symlinks and non-regular files, rechecks containment, opens
with no-follow semantics, matches the opened inode, reads once, and copies only
those validated bytes into the receipt bundle. Put both evidence and output
roots under `RUNNER_TEMP` so receipt creation does not dirty the qualified
source. Exit `0` records `PASS`, exit `3` records `NQ`, and any other command
exit records `FAIL`. A typed producer outcome that disagrees with the exit,
missing evidence, a launch failure, or an identity change records `FAIL`.

One command may claim several scenarios, but each claim must be written as
`STABLE_ID:GATE:SCENARIO` and use the item-specific allowed producer and
evidence kind. A broad `cargo test`, `/usr/bin/true`, a shell string, a dry-run,
or a raw MCP client cannot be wrapped and relabelled as a producer receipt.
Producer entrypoints may invoke their owned lower-level commands, but must map
each assertion to the exact scenario and write the typed result themselves.

This repository must not check in generated receipts or a precomputed `PASS`.
Receipts are per-run artifacts bound to the executing source SHA.

`qualification_aggregate.py` loads every receipt in one bundle, revalidates the
producer command contract, structured result, subjects, policy hashes, and
copied evidence, then recomputes every item, gate, and scenario. The current
checkout and qualifying receipts must be clean and match the source SHA,
workflow run id, workflow run attempt, and producer job identity. GitHub
environment values cannot be overridden by CLI flags. The highest attempt is selected once for the whole
workflow run; attempt 2 cannot fill omitted scenarios from any attempt 1 job or
producer. A dirty or otherwise rejected latest attempt remains `NQ`, while
same-attempt non-identical receipts for one producer/scenario, even when they
claim different jobs, are
equivocation and make the framework `FAIL`.

Malformed, policy-invalid, conflicting, or hash-mismatched current receipts
make the framework `FAIL`. Missing evidence and explicit `NQ` remain `NQ`; any
required scenario `FAIL` makes its gate and item `FAIL`. Exit `0` means only
that all 47 items in this Workbench contract ledger are `PASS`; it is not an
all-pre-#423-feature or phase-1-through-6 qualification. Exit `3` means `NQ`,
and exit `2` means `FAIL`.

```bash
python3 scripts/workbench/qualification_aggregate.py \
  --repo "$GITHUB_WORKSPACE" \
  --receipt-dir "$RUNNER_TEMP/pre423-qualification/receipts" \
  --product-artifact-manifest \
    "$RUNNER_TEMP/pre423-qualification/product-artifacts.json" \
  --output "$RUNNER_TEMP/pre423-qualification/qualification.json"
```

Receipts contain hashes, not cryptographic signatures. The receipt runner and
aggregate prove internal consistency; a workflow file from the pull request is
not by itself a trust root. Live receipts are therefore ineligible without a
closed `nokv.pre423.product_artifact_manifest.v1` supplied outside both the
checkout and receipt bundle. The manifest binds every live producer and job to
the current workflow run, attempt, head SHA, immutable artifact id and digest,
and the exact product-binary member path and digest. Missing provenance keeps
live results `NQ`; malformed, duplicate, stale, or conflicting provenance is a
framework `FAIL`.

The external manifest is still a claim, not a signature. A merge-blocking
deployment must obtain it from a protected reusable or required workflow whose
definition is outside pull-request control, or from a protected GitHub App or
external broker. That boundary must download the complete current-attempt
bundles, verify the provider artifact identities, and pass the closed manifest
to the aggregate. The current repository workflow does not yet establish that
external boundary, so local or pull-request-controlled aggregate output cannot
qualify live scenarios as trusted release evidence.

## Honest coverage of current commands

The framework does not infer stable IDs from a broad command name. Producer
wiring must list the exact scenarios that the command asserts today. Five
source-bound producers are checked in for static or exact Rust-test evidence:
`api-absence`, `api-decision`, `commit-replay`, `cursor-differential`, and
`nokv-agent-unit`. Rust producers additionally bind the actual Cargo and rustc
toolchain binaries and verbose-version output; externally supplied `CARGO`,
`PATH`, `HOME`, wrapper, flag, and target-directory overrides cannot select the
qualified toolchain. These producers are not substitutes for the six live or
integration producers, and the current workflow does not yet collect all
eleven producers through the external provenance boundary. Therefore no
current command sequence qualifies the complete ledger even where individual
typed producers pass.

| Current command or script | Honest qualification boundary |
| --- | --- |
| `pre423_contract_ledger.py`, `workbench_contract_test.py`, and every `*_gate_test.py` | Validate policy, checker, or harness shape only. They sign no product stable ID by themselves. |
| `cargo test -p nokv-agent` | Candidate `nokv-agent-unit` source for schema-surface `T01`-`T04`, `C01`, `C02`, `C07`, and `L01`. |
| `cargo test -p nokv-agent --test sdk_facade` | Candidate `nokv-agent-unit` source for the ledger's facade-contract and output-golden scenarios. Each claim still needs a direct assertion-to-scenario mapping; the broad command is not one receipt for all IDs. |
| `live_workbench.py` | Its explicit stable checks can back native scenarios for `T08`, `C04`, `C05`, and `C15`. Its `C06` probe proves same-name read/write isolation, reconnect, and wrong-agent admission, but does not yet cover every operation required by the `C06` root-authority scenario. It is raw MCP evidence, not LingTai evidence. |
| `restore_composition_gate.py` | Can back restore-composition scenarios for `T14`, `T18`, `C20`, and `C21` where its exact A to snapshot A to B to snapshot B to C oracle asserts the scenario. It does not satisfy their independent provider, native, commit, output, or LingTai gates. |
| `object_namespace_recovery_gate.py` | Can back the provider restart binding scenario in `C06` and the explicit object-outage read scenario in `C12`. Its commit replay, wrong-prefix, and exact-byte observations are partial evidence only for the remaining provider scenarios. |
| `local_wal_recovery_gate.py` | Qualifies owner-epoch/local-WAL recovery, which is not one of the 47 pre-#423 stable IDs. It signs none of this ledger's scenarios. |

Until these commands are invoked by their exact source-bound producer, emit the
required typed result and evidence roles, and feed a required trusted aggregate
job, their existing success remains useful test evidence but is not a 47-item
Workbench ledger qualification result.

## Missing producer gates

The five source-bound static or exact-test producers are implemented and have
their own fail-closed policy tests. They still need to be executed as part of
the complete protected producer graph. Three existing live harnesses need
typed scenario/result/evidence integration without weakening their current
oracles: `live-workbench`, `object-namespace-recovery`, and
`restore-composition`. Three behavior boundaries still require dedicated
commands before the ledger can reach `PASS`:

1. `snapshot-lifecycle` integration must deterministically cover committed-only
   minting, frozen reads after live mutation, renew by id and alias, terminal
   retire replay, foreign identity, HistoryHold retention, reap, and reference
   release. Reap should use an injected clock or explicit lifecycle advance,
   never a timed sleep.
2. `lingtai-mcp` live must record the installed LingTai identity, launch the
   digest-bound NoKV binary through the real MCPClient, verify exactly 18 tools,
   exercise the item-specific restore, cursor, root-isolation, and restored
   composition scenarios, restart NoKV, reconnect, and recheck bytes and
   authority. A raw JSON-RPC client cannot emit this producer identity.
3. `python-sdk` live must record the installed package identity and execute the
   closed `L03`-`L06` oracle: exact bytes, range-batch ordering, retry,
   concurrency, queries, generations, rename/remove, frozen snapshots; the
   Workbench-scoped fsspec mode/write/read/range/collision behavior; checkpoint
   manifest commit-point and partial-step visibility; and torch DCP rank,
   short-batch, and multi-rank exact-byte behavior. Absence tests may cover old
   filesystem-shaped type names or POSIX emulation, but cannot replace these
   live behavior gates.

The intended required CI graph is:

```text
clean checkout at source SHA
  -> ledger and framework unit tests
  -> build digest-bound product and test-support artifacts
  -> parallel static, unit, integration, and live producers
  -> upload complete receipt bundles even on FAIL or NQ
  -> protected workflow or GitHub App verifies immutable artifact provenance
  -> emit the closed external product-artifact manifest
  -> aggregate current-attempt bundles with that manifest
  -> revalidate all bundles for the same source SHA and workflow run attempt
  -> required pre-#423 Workbench contract ledger qualification status
```

Provider, LingTai, Python, and restore work can run in parallel. Aggregation is
cheap; the cost is the existing product tests and live services. Splitting by
producer keeps a slow provider gate from being attached to an item that only
requires unit or static evidence while preserving every required gate already
listed in the ledger.
