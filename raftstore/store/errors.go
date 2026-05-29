// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	errNilStore                               = errors.New("raftstore: store is nil")
	errZeroRegionID                           = errors.New("raftstore: region id is zero")
	errZeroPeerID                             = errors.New("raftstore: peer id is zero")
	errNilPeerConfig                          = errors.New("raftstore: peer config is nil")
	errNilRouter                              = errors.New("raftstore: router is nil")
	errCommandApplyWithoutHandler             = errors.New("raftstore: command apply without handler")
	errNilCommand                             = errors.New("raftstore: command is nil")
	errRegionIDMissing                        = errors.New("raftstore: region id missing")
	errCommandPipelineUnavailable             = errors.New("raftstore: command pipeline unavailable")
	errReadCommandMissingRequests             = errors.New("raftstore: read command missing requests")
	errReadCommandNotReadOnly                 = errors.New("raftstore: read command must be read-only")
	errParentRegionIDZero                     = errors.New("raftstore: parent region id is zero")
	errChildRegionIDZero                      = errors.New("raftstore: child region id is zero")
	errChildRegionStartKeyRequired            = errors.New("raftstore: child region start key required")
	errSplitCommandMissingPayload             = errors.New("raftstore: split command missing payload")
	errMergeCommandMissingPayload             = errors.New("raftstore: merge command missing payload")
	errInvalidPeerChangeTarget                = errors.New("raftstore: invalid peer change target")
	errInvalidRegionIdentifiers               = errors.New("raftstore: invalid region identifiers")
	errRegionMetaNil                          = errors.New("raftstore: region meta is nil")
	errInvalidConfChangeContext               = errors.New("raftstore: invalid conf change context")
	errTransitionRegionIDZero                 = errors.New("raftstore: transition region id is zero")
	errTransitionProposalEmpty                = errors.New("raftstore: transition proposal is empty")
	errRaftMessageMissingRecipient            = errors.New("raftstore: raft message missing recipient")
	errSnapshotPeerBootstrapRequiresSnapshot  = errors.New("raftstore: snapshot peer bootstrap requires non-empty snapshot message")
	errSnapshotPeerBootstrapRequiresPeerBuild = errors.New("raftstore: snapshot peer bootstrap requires peer builder")
	errSnapshotPayloadMissingRegionMeta       = errors.New("raftstore: snapshot payload missing region metadata")
	errInstallRegionSnapshotRequiresSnapshot  = errors.New("raftstore: install region snapshot requires non-empty snapshot")
	errInstallRegionSnapshotRequiresPayload   = errors.New("raftstore: install region snapshot requires snapshot payload")
	errInstallRegionSnapshotRequiresPeerBuild = errors.New("raftstore: install region snapshot requires peer builder")
	errFailpointAfterSnapshotApply            = errors.New("raftstore: failpoint after snapshot apply before publish")
	errResolveLocksStartVersionRequired       = errors.New("raftstore: resolve locks start version is required")
	errEmptyResolveLockKey                    = errors.New("raftstore: empty resolve-lock key")
	errCheckTxnStatusPrimaryRequired          = errors.New("raftstore: primary key is required for check txn status")
	errTxnHeartBeatPrimaryRequired            = errors.New("raftstore: primary key is required for txn heartbeat")
)

// RegionRoutingError records stable region routing failures for store-local
// transaction maintenance commands.
type RegionRoutingError struct {
	Operation string
	RegionID  uint64
	Key       []byte
	Detail    string
	Err       error
}

func (e *RegionRoutingError) Error() string {
	if e == nil {
		return "raftstore: region routing error"
	}
	msg := "raftstore: region routing error"
	if e.Operation != "" {
		msg += " during " + e.Operation
	}
	if e.RegionID != 0 {
		msg += fmt.Sprintf(" for region %d", e.RegionID)
	}
	if len(e.Key) > 0 {
		msg += fmt.Sprintf(" key %x", e.Key)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *RegionRoutingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsRegionRoutingError(err error) bool {
	var target *RegionRoutingError
	return errors.As(err, &target)
}

func (e *RegionRoutingError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindRegionRouting
}

// ProtocolError records invalid command responses and transaction protocol
// violations that should not be retried as ordinary storage errors.
type ProtocolError struct {
	Operation string
	Detail    string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "raftstore: protocol error"
	}
	if e.Operation == "" {
		return "raftstore: protocol error: " + e.Detail
	}
	return "raftstore: " + e.Operation + " protocol error: " + e.Detail
}

func IsProtocolError(err error) bool {
	var target *ProtocolError
	return errors.As(err, &target)
}

func (e *ProtocolError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindProtocolViolation
}

func errNoRegionForKey(operation string, key []byte) error {
	return &RegionRoutingError{
		Operation: operation,
		Key:       append([]byte(nil), key...),
		Detail:    "no region covers key",
	}
}

func errRegionCommandFailed(operation string, regionID uint64, err any) error {
	return &RegionRoutingError{
		Operation: operation,
		RegionID:  regionID,
		Detail:    fmt.Sprintf("region command failed: %v", err),
	}
}

func errInvalidRegionCommandResponse(operation string, regionID uint64) error {
	return &ProtocolError{
		Operation: operation,
		Detail:    fmt.Sprintf("region %d returned invalid response", regionID),
	}
}

func errRegionKeyError(operation string, regionID uint64, keyErr *kvrpcpb.KeyError) error {
	err := nokverrors.NewTxnKeyError(keyErr)
	if err == nil {
		return nil
	}
	return nokverrors.Wrap(
		nokverrors.KindOfKeyError(keyErr),
		fmt.Sprintf("raftstore: %s region %d", operation, regionID),
		err,
	)
}

func errParentRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: parent region %d not found", id)
}
func errTargetRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: target region %d not found", id)
}
func errSourceRegionNotFound(id uint64) error {
	return fmt.Errorf("raftstore: source region %d not found", id)
}
func errRegionMetadataNotFound(id uint64) error {
	return fmt.Errorf("raftstore: region %d metadata not found", id)
}
func errInvalidRegionPeerID(regionID, peerID uint64) error {
	return fmt.Errorf("raftstore: invalid region (%d) or peer (%d) id", regionID, peerID)
}
func errPeerBuilderReturnedNilConfig(regionID uint64) error {
	return fmt.Errorf("raftstore: peer builder returned nil config for region %d", regionID)
}
func errRegionDoesNotAssignPeer(regionID, storeID uint64) error {
	return fmt.Errorf("raftstore: region %d does not assign a peer to store %d", regionID, storeID)
}
func errPeerAlreadyHosted(peerID, regionID uint64) error {
	return fmt.Errorf("raftstore: peer %d already hosted for region %d", peerID, regionID)
}
