// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/proof"
	"github.com/stretchr/testify/require"
)

func TestAdmissionLatchesSerializeOverlappingKeys(t *testing.T) {
	latches := NewAdmissionLatches()
	op := opWithValueWrites("dentry/a", "inode=7")

	release := latches.Lock(op)
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(op)
		close(entered)
		unlock()
		close(done)
	}()

	select {
	case <-entered:
		t.Fatal("overlapping admission entered while key was held")
	case <-time.After(20 * time.Millisecond):
	}
	release()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("overlapping admission did not resume")
	}
}

func TestAdmissionLatchesAllowDisjointKeys(t *testing.T) {
	latches := NewAdmissionLatches()
	release := latches.Lock(testGeneratedCreateOpForInodes(t, 9, 20, "a"))
	defer release()

	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(testGeneratedCreateOpForInodes(t, 10, 21, "b"))
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disjoint admission was blocked")
	}
}

func TestAdmissionLatchesUseGlobalKeyForPrefixPredicates(t *testing.T) {
	latches := NewAdmissionLatches()
	release := latches.Lock(testFootprintOp(compile.SemanticDelta{
		ReadPredicates: []compile.Predicate{{Kind: compile.PredicatePrefixScan, Key: []byte("dentry/")}},
	}))

	var entered atomic.Bool
	done := make(chan struct{})
	go func() {
		unlock := latches.Lock(opWithValueWrites("dentry/b", "inode=8"))
		entered.Store(true)
		unlock()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("global admission latch did not block ordinary key")
	case <-time.After(20 * time.Millisecond):
		require.False(t, entered.Load())
	}
	release()
	<-done
}

func TestAdmitRejectsFalseAdmission(t *testing.T) {
	err := Admit(context.Background(), testGeneratedCreateOp(t, "admit", "value"), func(context.Context, compile.MaterializedOp, AdmissionContext) (AdmissionResult, bool, error) {
		return AdmissionResult{}, false, nil
	}, AdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.ErrorIs(t, err, ErrAdmissionRejected)
}

func TestAdmitAndSealBindsGuardProofsAfterAdmission(t *testing.T) {
	op, err := generatedCreateIntentOp("guarded", "v", compile.WithQuotaMode(compile.QuotaModeEscrow))
	require.NoError(t, err)
	require.NoError(t, op.ValidateForAdmissionIntent())
	require.Error(t, op.ValidateForAdmission())

	sealed, err := AdmitAndSeal(context.Background(), op, func(context.Context, compile.MaterializedOp, AdmissionContext) (AdmissionResult, bool, error) {
		proofs := testPredicateProofsForMaterializedOp(op)
		guardProofs, err := compile.GuardProofsFor(op.CompiledOp, proofs, op.Delta.RuntimeGuards)
		require.NoError(t, err)
		return AdmissionResult{
			PredicateProofs: proofs,
			GuardProofs:     guardProofs,
		}, true, nil
	}, AdmissionContext{ProofFrontier: proof.ProofFrontier{EpochID: 1, Sequence: 1}})
	require.NoError(t, err)
	require.NoError(t, sealed.ValidateForAdmission())
	require.NotEmpty(t, sealed.GuardProofs)
}
