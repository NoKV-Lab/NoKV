// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package observe owns fsmeta's watch and snapshot observation surface.
//
// It defines runtime-neutral watch requests, events, cursors, subscriptions,
// apply notifications, and snapshot publication hooks. It may translate a
// resolved model mount into layout key prefixes, but it does not own namespace
// model objects, storage runtimes, protobuf conversion, or backend clients.
package observe
