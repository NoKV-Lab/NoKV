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

Each stable item records:

- class `A`, `B`, `C`, or `D` from the pre-test porting decision;
- current disposition: `restore`, `replace`, `retire`, or `do-not-restore`;
- the package or deployment owner and observable boundary;
- revision-relative source evidence; and
- the gates required before the item can be called recovered or deliberately
  retired.

Class A ports the public behavior unchanged. Class B preserves the invariant
but rewrites the test through `RootId`, path-native workspace types, immutable
artifact revisions, typed lifecycle operations, and current recovery fences.
Class C requires an explicit support/replacement/retirement contract. Class D
is excluded from the recovery backlog. A or B cannot be silently retired.

The ledger never makes FUSE, POSIX, fsspec, inode/dentry layout, a second
durable schema, or naked-offset pagination part of current product acceptance.
Where an old test contains one of those implementation details, retain only
the higher-level invariant and test it at the current owner boundary. In
particular, logical display roots do not establish isolation: the Agent
integration must persist distinct `RootId` authority.

Validate the ledger and its policy tests with:

```bash
python3 scripts/workbench/pre423_contract_ledger.py
python3 scripts/workbench/pre423_contract_ledger_test.py
```

Any recovery change should update the applicable item only when its listed
gate exists and passes at that boundary. Do not mark a schema-only check as
evidence for runtime behavior, restore composition, isolation, durability,
provider recovery, or real LingTai MCP integration.
