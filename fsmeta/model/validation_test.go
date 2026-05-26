// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package model

import "testing"

func TestValidateCreateRequest(t *testing.T) {
	req := CreateRequest{
		Mount:  "workspace",
		Parent: RootInode,
		Name:   "artifact",
		Attrs:  CreateAttrs{Type: InodeTypeFile},
	}
	if err := ValidateCreateRequest(req); err != nil {
		t.Fatalf("ValidateCreateRequest() error = %v", err)
	}

	req.Name = "."
	if err := ValidateCreateRequest(req); err != ErrInvalidName {
		t.Fatalf("ValidateCreateRequest(invalid name) error = %v, want %v", err, ErrInvalidName)
	}
}

func TestSnapshotTokenCloneDetachesEvidence(t *testing.T) {
	token := SnapshotSubtreeToken{
		Mount:       "workspace",
		MountKeyID:  1,
		RootInode:   RootInode,
		ReadVersion: 7,
		RuntimeEvidence: []SnapshotEvidenceRef{
			{EpochID: 3, EvidenceRoot: [32]byte{1}, PayloadDigest: [32]byte{2}},
		},
	}

	clone := token.Clone()
	clone.RuntimeEvidence[0].EpochID = 9

	if token.RuntimeEvidence[0].EpochID != 3 {
		t.Fatalf("Clone() shared evidence slice")
	}
}
