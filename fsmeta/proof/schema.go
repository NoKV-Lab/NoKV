// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package proof

type Version uint16

const Version1 Version = 1

type RuleID string

const (
	RulePredicatePointValue             RuleID = "predicate.point_value"
	RulePredicatePointAbsence           RuleID = "predicate.point_absence"
	RulePredicateOverlayFrontierAbsence RuleID = "predicate.overlay_frontier_absence"

	RuleGuardSingleLinkInode     RuleID = "guard.single_link_inode"
	RuleGuardSameAuthority       RuleID = "guard.same_authority"
	RuleGuardNonDirectoryInode   RuleID = "guard.non_directory_inode"
	RuleGuardEmptyDirectory      RuleID = "guard.empty_directory"
	RuleGuardLiveSession         RuleID = "guard.live_session"
	RuleGuardExpiredSessionOwner RuleID = "guard.expired_session_owner"
	RuleGuardQuotaCredit         RuleID = "guard.quota_credit"
)

type ProofFrontier struct {
	EpochID  uint64
	Sequence uint64
}

func (f ProofFrontier) Valid() bool {
	return f.EpochID != 0 && f.Sequence != 0
}

type Envelope struct {
	Version       Version
	Rule          RuleID
	InputsDigest  [32]byte
	SubjectDigest [32]byte
	BodyDigest    [32]byte
}

func EnvelopeDigest(envelope Envelope) [32]byte {
	h := newDigestBuilder()
	h.writeUint64(uint64(envelope.Version))
	h.writeString(string(envelope.Rule))
	h.writeBytes(envelope.InputsDigest[:])
	h.writeBytes(envelope.SubjectDigest[:])
	h.writeBytes(envelope.BodyDigest[:])
	return h.sum()
}
