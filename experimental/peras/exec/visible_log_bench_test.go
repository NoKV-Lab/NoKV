// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

func BenchmarkEncodeVisibleOperationRecord(b *testing.B) {
	record := benchmarkVisibleRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		payload, err := EncodeVisibleOperationRecord(record)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkEncodeVisibleOperationRecordTo(b *testing.B) {
	record := benchmarkVisibleRecord()
	scratch := make([]byte, 0, visibleOperationRecordEncodedSize(record))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		payload, err := EncodeVisibleOperationRecordTo(scratch[:0], record)
		if err != nil {
			b.Fatal(err)
		}
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func benchmarkVisibleRecord() VisibleOperationRecord {
	return VisibleOperationRecord{
		EpochID:           7,
		HolderID:          "holder-a",
		GrantID:           "grant-a",
		GrantExpiresNanos: 123456789,
		RootLineage:       VisibleRootLineage{ClusterEpoch: 1, Term: 2, Index: 3, Revision: 4},
		Scope: compile.AuthorityScope{
			Mount:      "m",
			MountKeyID: 1,
			Parents:    []model.InodeID{2},
		},
		Operation:         testVisibleReplayOperation(OperationID{ClientID: "client", Seq: 9}, []byte("a")),
		TimestampUnixNano: 1234,
	}
}
