// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package importboundary

import "testing"

func TestEvaluateCatchesForbiddenBoundaries(t *testing.T) {
	cases := []struct {
		name        string
		pkg         string
		imports     []string
		wantRule    string
		wantImport  string
		wantPackage string
	}{
		{
			name:        "root package importing distributed assembly",
			pkg:         ModulePath,
			imports:     []string{ModulePath + "/raftstore/mvcc"},
			wantRule:    "root package stays free of distributed assembly",
			wantImport:  ModulePath + "/raftstore/mvcc",
			wantPackage: ModulePath,
		},
		{
			name:        "fsmeta exec importing raftstore",
			pkg:         ModulePath + "/fsmeta/exec",
			imports:     []string{ModulePath + "/raftstore/client"},
			wantRule:    "fsmeta executor stays runtime-neutral",
			wantImport:  ModulePath + "/raftstore/client",
			wantPackage: ModulePath + "/fsmeta/exec",
		},
		{
			name:        "fsmeta exec importing protobuf",
			pkg:         ModulePath + "/fsmeta/exec",
			imports:     []string{ModulePath + "/pb/kv"},
			wantRule:    "fsmeta executor stays runtime-neutral",
			wantImport:  ModulePath + "/pb/kv",
			wantPackage: ModulePath + "/fsmeta/exec",
		},
		{
			name:        "fsmeta backend importing concrete runtime",
			pkg:         ModulePath + "/fsmeta/backend",
			imports:     []string{ModulePath + "/local"},
			wantRule:    "fsmeta backend stays storage-neutral",
			wantImport:  ModulePath + "/local",
			wantPackage: ModulePath + "/fsmeta/backend",
		},
		{
			name:        "raftstore snapshot protocol importing concrete storage engine",
			pkg:         ModulePath + "/raftstore/snapshot",
			imports:     []string{ModulePath + "/storage/pebble"},
			wantRule:    "raftstore snapshot protocol stays backend-neutral",
			wantImport:  ModulePath + "/storage/pebble",
			wantPackage: ModulePath + "/raftstore/snapshot",
		},
		{
			name:        "local db importing raftstore",
			pkg:         ModulePath + "/local/stats",
			imports:     []string{ModulePath + "/raftstore/stats"},
			wantRule:    "local db stays free of distributed assembly",
			wantImport:  ModulePath + "/raftstore/stats",
			wantPackage: ModulePath + "/local/stats",
		},
		{
			name:        "txn percolator importing raftstore",
			pkg:         ModulePath + "/txn/percolator",
			imports:     []string{ModulePath + "/raftstore/client"},
			wantRule:    "txn layer stays below distributed assembly",
			wantImport:  ModulePath + "/raftstore/client",
			wantPackage: ModulePath + "/txn/percolator",
		},
		{
			name:        "txn mvcc reaching back into percolator",
			pkg:         ModulePath + "/txn/mvcc",
			imports:     []string{ModulePath + "/txn/percolator"},
			wantRule:    "txn mvcc stays protocol-neutral",
			wantImport:  ModulePath + "/txn/percolator",
			wantPackage: ModulePath + "/txn/mvcc",
		},
		{
			name:        "local runtime taking global errors",
			pkg:         ModulePath + "/local/internal/commit",
			imports:     []string{ModulePath + "/errors"},
			wantRule:    "local runtime stays free of global error taxonomy",
			wantImport:  ModulePath + "/errors",
			wantPackage: ModulePath + "/local/internal/commit",
		},
		{
			name:        "storage engine taking global errors",
			pkg:         ModulePath + "/storage/pebble",
			imports:     []string{ModulePath + "/errors"},
			wantRule:    "storage engines stay below MVCC and distributed semantics",
			wantImport:  ModulePath + "/errors",
			wantPackage: ModulePath + "/storage/pebble",
		},
		{
			name:        "holt adapter taking fsmeta semantics",
			pkg:         ModulePath + "/storage/holt",
			imports:     []string{ModulePath + "/fsmeta/model"},
			wantRule:    "holt storage engine stays below MVCC and distributed semantics",
			wantImport:  ModulePath + "/fsmeta/model",
			wantPackage: ModulePath + "/storage/holt",
		},
		{
			name:        "runtime importing third-party holt checkout",
			pkg:         ModulePath + "/local",
			imports:     []string{ModulePath + "/third_party/holt"},
			wantRule:    "third-party holt checkout is adapter-only",
			wantImport:  ModulePath + "/third_party/holt",
			wantPackage: ModulePath + "/local",
		},
		{
			name:        "meta root reaching into coordinator",
			pkg:         ModulePath + "/meta/root/server",
			imports:     []string{ModulePath + "/coordinator/client"},
			wantRule:    "meta root does not depend on coordinator service layer",
			wantImport:  ModulePath + "/coordinator/client",
			wantPackage: ModulePath + "/meta/root/server",
		},
		{
			name:        "coordinator reaching into raftstore",
			pkg:         ModulePath + "/coordinator/balancer",
			imports:     []string{ModulePath + "/raftstore/store"},
			wantRule:    "coordinator stays free of raftstore execution packages",
			wantImport:  ModulePath + "/raftstore/store",
			wantPackage: ModulePath + "/coordinator/balancer",
		},
		{
			name:        "scheduling reaching into service layer",
			pkg:         ModulePath + "/coordinator/scheduling",
			imports:     []string{ModulePath + "/coordinator/server"},
			wantRule:    "coordinator scheduling stays policy-only",
			wantImport:  ModulePath + "/coordinator/server",
			wantPackage: ModulePath + "/coordinator/scheduling",
		},
		{
			name:        "storecontrol reaching into scheduling",
			pkg:         ModulePath + "/coordinator/storecontrol",
			imports:     []string{ModulePath + "/coordinator/scheduling"},
			wantRule:    "coordinator storecontrol stays out of scheduling and service",
			wantImport:  ModulePath + "/coordinator/scheduling",
			wantPackage: ModulePath + "/coordinator/storecontrol",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			violations := Evaluate(tc.pkg, tc.imports)
			if !hasViolation(violations, tc.wantRule, tc.wantPackage, tc.wantImport) {
				t.Fatalf("missing violation rule=%q package=%q import=%q in %#v",
					tc.wantRule, tc.wantPackage, tc.wantImport, violations)
			}
		})
	}
}

func TestEvaluateHonorsExactAndExemptScopes(t *testing.T) {
	cases := []struct {
		name    string
		pkg     string
		imports []string
	}{
		{
			name:    "exact-match rule does not apply to subpackages",
			pkg:     ModulePath + "/cmd/nokv",
			imports: []string{ModulePath + "/raftstore/mvcc"},
		},
		{
			name:    "fsmeta client outside fsmeta/exec scope",
			pkg:     ModulePath + "/fsmeta/client",
			imports: []string{ModulePath + "/raftstore/client"},
		},
		{
			name:    "local/errkind exempt from local error rule",
			pkg:     ModulePath + "/local/errkind",
			imports: []string{ModulePath + "/errors"},
		},
		{
			name: "storecontrol importing client is allowed",
			pkg:  ModulePath + "/coordinator/storecontrol",
			imports: []string{
				ModulePath + "/coordinator/client",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Evaluate(tc.pkg, tc.imports); len(got) != 0 {
				t.Fatalf("expected no violations, got %#v", got)
			}
		})
	}
}

func TestPathMatchesIsPrefixSafe(t *testing.T) {
	if !PathMatches(ModulePath+"/raftstore/client", ModulePath+"/raftstore") {
		t.Fatal("child package should match prefix")
	}
	if PathMatches(ModulePath+"/raftstorex", ModulePath+"/raftstore") {
		t.Fatal("sibling prefix sharing leading characters must not match")
	}
	if !PathMatches(ModulePath+"/raftstore", ModulePath+"/raftstore") {
		t.Fatal("exact package should match")
	}
}

func hasViolation(violations []Violation, rule, pkg, imp string) bool {
	for _, v := range violations {
		if v.Rule == rule && v.Package == pkg && v.Import == imp {
			return true
		}
	}
	return false
}
