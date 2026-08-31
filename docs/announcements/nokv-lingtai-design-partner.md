<p align="center">
  <img src="../public/img/community/nokv-lingtai-banner-en.png" alt="NoKV × LingTai — Design Partner Collaboration" width="100%" />
</p>

# NoKV × LingTai: a design-partner collaboration

> Status: this page records the start of the collaboration. The active
> integration uses the complete
> [Workbench contract](../workbench-contract.md) through the native full CLI
> first, the direct Python SDK second, and an optional MCP sidecar when a host
> requires it, backed by NoKV's path-native workspace format. It does not use
> NoKV as a FUSE/POSIX mount.
> See [Product Design](../product-design.md).
>
> Update (2026-08): the optional Workbench MCP sidecar described on this page
> is deprecated and is not a supported NoKV integration surface. The stable
> boundary was and remains the 18-tool Workbench semantic contract, reached
> through the native full CLI and the direct Python SDK. The text below is
> preserved as published.

This announcement marks the start of the design-partner collaboration between
**NoKV** and **LingTai**
([Lingtai-AI/lingtai](https://github.com/Lingtai-AI/lingtai)).

## Two projects, one shared workflow

- **LingTai** is a local-first Agent runtime whose projects organize state,
  mailboxes, logs, and artifacts through path-shaped local files.
- **NoKV** is a distributed Agent workspace and artifact store. It exposes
  path-shaped identities through its native CLI, direct SDKs, and an optional
  Workbench MCP sidecar while
  storing canonical metadata in Holt and immutable bodies in S3-compatible
  storage.

The integration point is the Workbench contract, not a shared host-filesystem
namespace. LingTai owns its local runtime layout; NoKV owns distributed
artifact identity, publication, discovery, and recovery semantics.

## What we're building together

The collaboration focuses on:

- **Recovery and durable reuse**: leased snapshots provide short recovery,
  while immutable commits/tags retain long-lived artifact sets and restore
  creates a new Workbench.
- **Atomic, crash-consistent publishing**: concurrent Agent writes and a
  mid-run crash never leave a half-written workspace.
- **Artifact provenance**: versioned blocks with digests keep a derived
  artifact traceable to the run that produced it.
- **A queryable metadata layer**: ask *"what produced this / what depends on
  this"* across an Agent's outputs.

The upper path-shaped behavior remains stable while the storage boundary stays
explicit. Executables that require local files use materialize/collect adapters;
that sandbox is not NoKV namespace truth.

## Current direction

The stable boundary is the 18-tool Workbench semantic contract. Downstream
skills use the native full CLI by default; embedded callers use the Python SDK;
MCP remains an optional sidecar transport. NoKV keeps path-shaped Agent
semantics while storing canonical full-path metadata in Holt and immutable
artifact revisions in S3-compatible storage. LingTai remains the active design
partner and first-client integration.

If a stateful, snapshot-able, auditable Agent workspace is something you've
wanted: star NoKV, follow [LingTai](https://github.com/Lingtai-AI/lingtai), and
watch this space.

## Contact

- NoKV: hello@nokv.io
- LingTai: lingtai2026@gmail.com

## Join the community

<img src="../public/img/community/lingtai-seal.svg" width="72" alt="LingTai seal" />

- Discord (NoKV): https://discord.gg/c5PZapnwPh
- Slack (NoKV): the NoKV channel in the CNCF community Slack (join at https://slack.cncf.io, then open https://cloud-native.slack.com/archives/C0BBDBYE3H6 )
- WeChat group (LingTai): email lingtai2026@gmail.com for community details
