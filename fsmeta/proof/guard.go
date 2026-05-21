// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package proof

type GuardEvidenceKind uint8

const (
	GuardEvidenceUnknown             GuardEvidenceKind = 0
	GuardEvidenceSingleLinkInode     GuardEvidenceKind = 1
	GuardEvidenceSameAuthority       GuardEvidenceKind = 2
	GuardEvidenceNonDirectoryInode   GuardEvidenceKind = 3
	GuardEvidenceLiveSession         GuardEvidenceKind = 4
	GuardEvidenceExpiredSessionOwner GuardEvidenceKind = 5
	GuardEvidenceQuotaCredit         GuardEvidenceKind = 6
	GuardEvidenceEmptyDirectory      GuardEvidenceKind = 7
)

type GuardEvidence struct {
	SchemaVersion        Version
	Guard                RuleID
	Kind                 GuardEvidenceKind
	DescriptorDigest     [32]byte
	PredicateProofDigest [32]byte
	FootprintDigest      [32]byte
	EffectDigest         [32]byte
	SubjectDigest        [32]byte
	ProofFrontier        ProofFrontier
}

type GuardProof struct {
	SchemaVersion Version
	Guard         RuleID
	Passed        bool
	Evidence      GuardEvidence
	Digest        [32]byte
}

type GuardObligation struct {
	Guard  RuleID
	Digest [32]byte
}

func GuardObligationDigest(guard RuleID) [32]byte {
	h := newDigestBuilder()
	h.writeString("fsmeta-proof/guard-obligation")
	h.writeUint64(uint64(Version1))
	h.writeString(string(guard))
	return h.sum()
}

func GuardProofDigest(guard RuleID, passed bool, evidence GuardEvidence) [32]byte {
	h := newDigestBuilder()
	h.writeString("fsmeta-proof/guard-proof")
	h.writeUint64(uint64(Version1))
	h.writeString(string(guard))
	h.writeBool(passed)
	h.writeUint64(uint64(evidence.SchemaVersion))
	h.writeString(string(evidence.Guard))
	h.writeUint64(uint64(evidence.Kind))
	h.writeBytes(evidence.DescriptorDigest[:])
	h.writeBytes(evidence.PredicateProofDigest[:])
	h.writeBytes(evidence.FootprintDigest[:])
	h.writeBytes(evidence.EffectDigest[:])
	h.writeBytes(evidence.SubjectDigest[:])
	h.writeUint64(evidence.ProofFrontier.EpochID)
	h.writeUint64(evidence.ProofFrontier.Sequence)
	return h.sum()
}

func GuardProofFor(guard RuleID, passed bool, evidence GuardEvidence) GuardProof {
	return GuardProof{
		SchemaVersion: Version1,
		Guard:         guard,
		Passed:        passed,
		Evidence:      evidence,
		Digest:        GuardProofDigest(guard, passed, evidence),
	}
}

func VerifyGuardProof(obligation GuardObligation, expected GuardEvidence, proof GuardProof) error {
	if proof.SchemaVersion != Version1 || proof.Evidence.SchemaVersion != Version1 {
		return ErrInvalidProof
	}
	if proof.Guard != obligation.Guard || !proof.Passed {
		return ErrInvalidProof
	}
	if obligation.Digest != ([32]byte{}) && obligation.Digest != GuardObligationDigest(obligation.Guard) {
		return ErrInvalidProof
	}
	if proof.Evidence.Guard != proof.Guard {
		return ErrInvalidProof
	}
	if proof.Evidence != expected {
		return ErrInvalidProof
	}
	if proof.Digest != GuardProofDigest(proof.Guard, proof.Passed, proof.Evidence) {
		return ErrInvalidProof
	}
	return nil
}
