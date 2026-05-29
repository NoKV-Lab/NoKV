// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

// UserKeyShape describes runtime-derived key structure that the local engine
// can use without importing higher-level metadata packages. Shape data is not
// persisted; it only guides local commit-shard routing.
type UserKeyShape struct {
	// LocalityPrefix groups keys that should share one local apply group.
	LocalityPrefix []byte
	// ShardKey is the exact byte sequence used for local shard routing. When
	// empty, LocalityPrefix is used; when both are empty the full user key is
	// hashed by the generic router.
	ShardKey []byte
	// Family is a low-cardinality key-family marker for diagnostics.
	Family byte
}

// UserKeyShapeExtractor returns the shape for one user key. Returned slices may
// alias userKey and are consumed synchronously by the caller. The extractor
// must be deterministic and must not depend on process-local mutable state
// because WAL recovery and AtomicMutate admission use the same local apply
// boundary.
type UserKeyShapeExtractor func(userKey []byte) UserKeyShape

func (s UserKeyShape) shardKey() []byte {
	if len(s.ShardKey) > 0 {
		return s.ShardKey
	}
	return s.LocalityPrefix
}
