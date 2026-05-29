<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Thermos

`experimental/thermos` is NoKV's optional internal hotspot detector. It is no
longer part of the default read path or physical storage-backend planning.

## Current Role

The production role is deliberately narrow:

- detect write-hot keys when `Options.ThermosEnabled` is enabled
- enforce `Options.WriteHotKeyLimit` via `Thermos.TouchAndClamp`
- publish write-hot snapshots through `StatsSnapshot.Hot.WriteKeys` and `StatsSnapshot.Hot.WriteRing`

If `ThermosEnabled=false`, the DB runs without Thermos tracking on the hot path.

## What Thermos Does Not Control Anymore

These integrations were intentionally removed from the default engine path:

- read-path hot tracking and asynchronous read prefetch
- physical compaction scoring inside Pebble or Holt
- legacy value placement experiments
- hot-write batch enlargement heuristics

NoKV now stores values through the selected raw storage backend, so Thermos does
not participate in value placement.

## Data Structure

Thermos remains a concurrent in-memory frequency tracker with:

- sharded hash buckets
- lock-free bucket lists for lookup/insert
- atomic counters per node
- optional sliding-window and rotation support

These capabilities are still useful for optional write throttling and operational diagnostics, even though they are no longer wired into unrelated subsystems.

## Relevant Options

| Option | Meaning |
| --- | --- |
| `ThermosEnabled` | Master switch for write-hot tracking. |
| `WriteHotKeyLimit` | Reject writes once a single key exceeds the configured threshold. |
| `ThermosBits` | Bucket count (`2^bits`) for the tracker. |
| `ThermosTopK` | Number of hot keys exported in stats. |
| `ThermosRotationInterval` | Optional dual-ring rotation. |
| `ThermosWindowSlots` / `ThermosWindowSlotDuration` | Optional sliding-window tracking. |
| `ThermosNodeCap` / `ThermosNodeSampleBits` | Bound in-memory growth. |

## Write Throttling

When both `ThermosEnabled` and `WriteHotKeyLimit > 0` are set, NoKV records write frequency by `CF + UserKey` and returns `utils.ErrHotKeyWriteThrottle` once the limit is reached.

This path exists to protect metadata execution from pathological skew. It is
intentionally independent from backend cache warming and compaction.

## Stats

Thermos contributes only write-side observability:

- `StatsSnapshot.Hot.WriteKeys`
- `StatsSnapshot.Hot.WriteRing`
- `StatsSnapshot.Write.HotKeyLimited`

The CLI surfaces the same data under `nokv stats`.

## Design Position

Thermos should be understood as:

- an experimental internal detector
- an optional write throttling tool
- not a required performance feature
- not a default read-path optimization

That narrower scope keeps the core engine path simpler and makes Thermos easier to reason about.
