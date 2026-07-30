<p align="center">
  <img src="../public/img/community/nokv-lingtai-banner-en.png" alt="NoKV × Lingtai — Design Partner Collaboration" width="100%" />
</p>

# NoKV × Lingtai: a design-partner collaboration

Published 23 June 2026. Integration status updated 30 July 2026.

**NoKV** and **Lingtai**
([Lingtai-AI/lingtai](https://github.com/Lingtai-AI/lingtai)) are working together
as design partners on durable workspaces for long-running agents. This page
records the collaboration and its technical boundary; it is not a statement of
production readiness.

## Two projects, one filesystem-shaped workspace

- **Lingtai** is a local-first Agent runtime in which long-lived agents keep
  state, mailboxes, logs, and artifacts in on-disk project directories that
  remain inspectable with ordinary file tools. Lingtai reports an active early
  developer community.
- **NoKV** is a durable metadata control plane for object-backed multi-agent
  workspaces. It provides a filesystem-shaped namespace, shard-local atomic
  publication, leased historical snapshots, and CoW restore-to-fork primitives
  while leaving planning, semantic memory, and orchestration to the Agent
  runtime.

Lingtai gives an Agent a filesystem-shaped home. NoKV provides storage and
metadata primitives that can make such a workspace durable, recoverable, and
auditable without replacing plain-file access.

## What we are validating together

- **Workspace checkpoints and recovery:** pin a stable historical view and
  restore a committed workspace into a new CoW destination instead of mutating
  the source in place.
- **Shard-local crash-consistent publication:** publish an artifact or a group
  of checkpoint files atomically within one metadata owner.
- **Explicit provenance:** preserve digests and runtime-supplied provenance
  fields so an artifact can be linked to the run metadata that produced it.
- **Queryable workspace metadata:** search metadata recorded by the runtime and
  application. NoKV does not infer a semantic dependency graph on its own.

The NoKV-side Workbench MCP adapter, guarded 18-tool LingTai contract, leased
snapshot lifecycle, and durable restore acceptance path now exist. Availability
inside a particular Lingtai distribution remains release-, capability-, and
preflight-dependent.

## Current boundary

- The raw Workbench profile has 17 base tools; `workbench_restore` is exposed as
  the eighteenth only when every relevant owner confirms the capability.
- Snapshot pins are leased. A checkpoint name is a discoverability alias, not a
  permanent GC root or a freeze of the live workspace.
- Restore-to-fork is same-shard only and leaves the source unchanged. NoKV does
  not currently provide a cross-shard atomic restore or publication transaction.
- Workbench path scoping is not authentication, RBAC, or tenant policy.
  Production-grade identity boundaries, live workspace freezing, and metadata
  high availability require separate hardening.

Both projects remain pre-1.0 and are evolving quickly. We will publish workload
evidence and downstream availability separately as they become reproducible.

## Contact

- NoKV: hello@nokv.io
- Lingtai: lingtai2026@gmail.com

## Join the community

<img src="../public/img/community/lingtai-seal.svg" width="72" alt="LingTai seal" />

- Discord (NoKV): https://discord.gg/c5PZapnwPh
- Slack (NoKV): the NoKV channel in the CNCF community Slack (join at https://slack.cncf.io, then open https://cloud-native.slack.com/archives/C0BBDBYE3H6 )
- WeChat group (Lingtai): email lingtai2026@gmail.com for community details
