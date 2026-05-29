// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package importboundary enforces NoKV's package-import direction rules from
// docs/development/code_contract.md §2. Ownership of truth — not file count or
// convenience — drives which package may import which. The rules were
// originally tracked in architecture/dependencies.go and have been folded into
// the unified lint pipeline.
package importboundary

import "strings"

// ModulePath is the canonical Go module path used by all import-direction
// rules. Keeping it a public constant lets tests construct synthetic packages
// without relying on the go list driver.
const ModulePath = "github.com/feichai0017/NoKV"

// Rule describes one import-direction constraint. PackagePrefix selects which
// packages the rule applies to; Forbidden lists package paths that those
// packages may not import. Exempt opts specific sub-paths out of the rule.
//
// When PackageExact is true PackagePrefix matches one package only; otherwise
// it matches the prefix and every sub-package.
type Rule struct {
	Name          string
	PackagePrefix string
	PackageExact  bool
	Forbidden     []string
	Exempt        []string
}

// Rules is the full ordered list of import-direction constraints. The order is
// significant only for deterministic diagnostic output: each rule is checked
// independently against every (package, import) pair.
//
// New rules must record the contract section they enforce and keep
// PackagePrefix anchored at ModulePath so the rule scope cannot be widened by
// accident.
var Rules = []Rule{
	{
		Name:          "root package stays free of distributed assembly",
		PackagePrefix: ModulePath,
		PackageExact:  true,
		Forbidden: []string{
			ModulePath + "/fsmeta",
			ModulePath + "/raftstore/localmeta",
			ModulePath + "/raftstore/raftlog",
			ModulePath + "/raftstore/snapshot",
			ModulePath + "/raftstore/mvcc",
			ModulePath + "/raftstore/mode",
		},
	},
	{
		Name:          "local db stays free of distributed assembly",
		PackagePrefix: ModulePath + "/local",
		Forbidden: []string{
			ModulePath + "/fsmeta",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/raftstore",
			ModulePath + "/txn/percolator",
		},
	},
	{
		Name:          "txn layer stays below distributed assembly",
		PackagePrefix: ModulePath + "/txn",
		Forbidden: []string{
			ModulePath + "/fsmeta",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/raftstore",
		},
	},
	{
		Name:          "txn mvcc stays protocol-neutral",
		PackagePrefix: ModulePath + "/txn/mvcc",
		Forbidden: []string{
			ModulePath + "/txn/percolator",
			ModulePath + "/txn/latch",
		},
	},
	{
		Name:          "txn storage stays protocol-neutral",
		PackagePrefix: ModulePath + "/txn/storage",
		Forbidden: []string{
			ModulePath + "/txn/percolator",
			ModulePath + "/txn/latch",
			ModulePath + "/txn/mvcc",
		},
	},
	{
		Name:          "txn latch stays protocol-neutral",
		PackagePrefix: ModulePath + "/txn/latch",
		Forbidden: []string{
			ModulePath + "/txn/percolator",
			ModulePath + "/txn/mvcc",
			ModulePath + "/txn/storage",
		},
	},
	{
		Name:          "local runtime stays free of global error taxonomy",
		PackagePrefix: ModulePath + "/local",
		Forbidden: []string{
			ModulePath + "/errors",
		},
		Exempt: []string{
			ModulePath + "/local/errkind",
		},
	},
	{
		Name:          "storage backend contract stays semantics-free",
		PackagePrefix: ModulePath + "/storage/kv",
		Forbidden: []string{
			ModulePath + "/errors",
			ModulePath + "/fsmeta",
			ModulePath + "/txn",
			ModulePath + "/raftstore",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "storage engines stay below MVCC and distributed semantics",
		PackagePrefix: ModulePath + "/storage/pebble",
		Forbidden: []string{
			ModulePath + "/errors",
			ModulePath + "/fsmeta",
			ModulePath + "/txn",
			ModulePath + "/raftstore",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "memory storage engines stay below MVCC and distributed semantics",
		PackagePrefix: ModulePath + "/storage/memory",
		Forbidden: []string{
			ModulePath + "/errors",
			ModulePath + "/fsmeta",
			ModulePath + "/txn",
			ModulePath + "/raftstore",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "holt storage engine stays below MVCC and distributed semantics",
		PackagePrefix: ModulePath + "/storage/holt",
		Forbidden: []string{
			ModulePath + "/errors",
			ModulePath + "/fsmeta",
			ModulePath + "/txn",
			ModulePath + "/raftstore",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/pb",
		},
	},
	{
		Name:          "third-party holt checkout is adapter-only",
		PackagePrefix: ModulePath,
		Forbidden: []string{
			ModulePath + "/third_party/holt",
		},
	},
	{
		Name:          "utils stays free of global error taxonomy",
		PackagePrefix: ModulePath + "/utils",
		Forbidden: []string{
			ModulePath + "/errors",
		},
	},
	{
		Name:          "fsmeta executor stays runtime-neutral",
		PackagePrefix: ModulePath + "/fsmeta/exec",
		Forbidden: []string{
			ModulePath + "/pb",
			ModulePath + "/coordinator",
			ModulePath + "/raftstore",
			ModulePath + "/meta/root",
			ModulePath + "/local",
		},
	},
	{
		Name:          "fsmeta backend stays storage-neutral",
		PackagePrefix: ModulePath + "/fsmeta/backend",
		Forbidden: []string{
			ModulePath + "/pb",
			ModulePath + "/local",
			ModulePath + "/raftstore",
			ModulePath + "/coordinator",
			ModulePath + "/meta/root",
			ModulePath + "/experimental/peras",
		},
	},
	{
		Name:          "fsmeta watch router stays store-neutral",
		PackagePrefix: ModulePath + "/fsmeta/exec/watch",
		Forbidden: []string{
			ModulePath + "/raftstore/store",
		},
	},
	{
		Name:          "raftstore snapshot protocol stays backend-neutral",
		PackagePrefix: ModulePath + "/raftstore/snapshot",
		PackageExact:  true,
		Forbidden: []string{
			ModulePath + "/storage/pebble",
			ModulePath + "/storage/memory",
			ModulePath + "/storage/wal",
			ModulePath + "/storage/file",
			ModulePath + "/local",
			ModulePath + "/fsmeta",
		},
	},
	{
		Name:          "meta root does not depend on coordinator service layer",
		PackagePrefix: ModulePath + "/meta/root",
		Forbidden: []string{
			ModulePath + "/coordinator",
		},
	},
	{
		Name:          "coordinator stays free of raftstore execution packages",
		PackagePrefix: ModulePath + "/coordinator",
		Forbidden: []string{
			ModulePath + "/raftstore",
		},
	},
	{
		Name:          "coordinator scheduling stays policy-only",
		PackagePrefix: ModulePath + "/coordinator/scheduling",
		Forbidden: []string{
			ModulePath + "/coordinator/client",
			ModulePath + "/coordinator/server",
			ModulePath + "/coordinator/storecontrol",
		},
	},
	{
		Name:          "coordinator storecontrol stays out of scheduling and service",
		PackagePrefix: ModulePath + "/coordinator/storecontrol",
		Forbidden: []string{
			ModulePath + "/coordinator/scheduling",
			ModulePath + "/coordinator/server",
		},
	},
}

// Violation describes one (package, import) pair that broke a Rule.
type Violation struct {
	Rule    string
	Package string
	Import  string
}

// Evaluate reports every Rule that the package at pkgPath violates given its
// declared importPaths. The pure function exists so unit tests can exercise the
// matching logic without spinning up a Go analyzer Pass.
func Evaluate(pkgPath string, importPaths []string) []Violation {
	var out []Violation
	for _, rule := range Rules {
		if !rule.MatchesPackage(pkgPath) || rule.IsExempt(pkgPath) {
			continue
		}
		for _, imp := range importPaths {
			for _, forbidden := range rule.Forbidden {
				if PathMatches(imp, forbidden) {
					out = append(out, Violation{
						Rule:    rule.Name,
						Package: pkgPath,
						Import:  imp,
					})
				}
			}
		}
	}
	return out
}

// MatchesPackage reports whether pkgPath is in the rule's scope.
func (r Rule) MatchesPackage(pkgPath string) bool {
	if r.PackageExact {
		return pkgPath == r.PackagePrefix
	}
	return PathMatches(pkgPath, r.PackagePrefix)
}

// IsExempt reports whether pkgPath is explicitly opted out of the rule.
func (r Rule) IsExempt(pkgPath string) bool {
	for _, exempt := range r.Exempt {
		if PathMatches(pkgPath, exempt) {
			return true
		}
	}
	return false
}

// PathMatches treats prefix as a Go package path prefix. The prefix matches the
// exact path or any sub-package; sibling packages whose name happens to share
// the same leading characters are not treated as matches.
func PathMatches(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
