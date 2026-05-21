// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package proof

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPredicateProofIsVersionedAndSelfConsistent(t *testing.T) {
	p := NewPredicateProof([]byte("key"), []byte("value"), true, 7, ReadSourceBase, ProofFrontier{})
	require.Equal(t, Version1, p.SchemaVersion)
	require.Equal(t, RulePredicatePointValue, p.Rule)
	require.Equal(t, PredicateProofPointValue, p.ProofKind)
	require.NoError(t, VerifyPredicateProof(p))

	p.ScopeDigest[0] ^= 0xff
	require.ErrorIs(t, VerifyPredicateProof(p), ErrInvalidProof)
}

func TestGuardProofVerifierBindsExpectedEvidence(t *testing.T) {
	evidence := GuardEvidence{
		SchemaVersion: Version1,
		Guard:         RuleGuardLiveSession,
		Kind:          GuardEvidenceLiveSession,
		ProofFrontier: ProofFrontier{EpochID: 1, Sequence: 2},
	}
	obligation := GuardObligation{Guard: RuleGuardLiveSession, Digest: GuardObligationDigest(RuleGuardLiveSession)}
	proof := GuardProofFor(RuleGuardLiveSession, true, evidence)
	require.NoError(t, VerifyGuardProof(obligation, evidence, proof))

	evidence.SubjectDigest[0] ^= 0xff
	require.ErrorIs(t, VerifyGuardProof(obligation, evidence, proof), ErrInvalidProof)
}

func TestGuardEvidenceKindValuesAreStable(t *testing.T) {
	require.Equal(t, GuardEvidenceKind(0), GuardEvidenceUnknown)
	require.Equal(t, GuardEvidenceKind(1), GuardEvidenceSingleLinkInode)
	require.Equal(t, GuardEvidenceKind(2), GuardEvidenceSameAuthority)
	require.Equal(t, GuardEvidenceKind(3), GuardEvidenceNonDirectoryInode)
	require.Equal(t, GuardEvidenceKind(4), GuardEvidenceLiveSession)
	require.Equal(t, GuardEvidenceKind(5), GuardEvidenceExpiredSessionOwner)
	require.Equal(t, GuardEvidenceKind(6), GuardEvidenceQuotaCredit)
	require.Equal(t, GuardEvidenceKind(7), GuardEvidenceEmptyDirectory)
}
