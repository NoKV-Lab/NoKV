// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package model owns fsmeta's storage-engine-neutral namespace model.
//
// It defines the public inode/dentry/session/quota/snapshot domain objects,
// operation request/result shapes, and validation rules. It does not own
// storage key encoding, protobuf conversion, raftstore commands, Percolator
// timestamps, or any concrete backend runtime.
package model
