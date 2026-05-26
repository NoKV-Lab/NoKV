// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	nokverrors "github.com/feichai0017/NoKV/errors"
	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

func (c *Runtime) RecoverWitnessSegments(ctx context.Context, scope compile.AuthorityScope, epochID uint64) error {
	if c == nil || epochID == 0 || c.installer == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.LoadRootSealedSegments(ctx, scope); err != nil {
		return err
	}
	records, err := c.collectWitnessSegments(ctx, epochID)
	if err != nil {
		return c.recordErrorf("probe peras segment witnesses: %w", err)
	}
	for _, record := range records {
		if c.segmentInstalled(record.SegmentRoot) {
			c.metrics.recoverySkipTotal.Add(1)
			continue
		}
		if err := fsperas.VerifySegmentWitnessRecord(record); err != nil {
			return c.recordErrorf("verify peras segment witness: %w", err)
		}
		segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.SegmentRoot, record.SegmentPayloadDigest)
		if err != nil {
			return c.recordErrorf("decode peras witness segment: %w", err)
		}
		if !SegmentWithinScope(segment, scope) {
			continue
		}
		stats := segment.Stats()
		if record.OperationCount != stats.OperationCount || record.EntryCount != stats.EntryCount {
			return c.recordError(fsperas.ErrInvalidWitnessRecord)
		}
		install, err := fsperas.PerasSegmentInstallPlan(segment, false)
		if err != nil {
			return c.recordErrorf("plan recovered peras segment install: %w", err)
		}
		job := perasFlushJob{
			scope:   scope,
			segment: segment,
			payload: record.SegmentPayload,
			digest:  record.SegmentPayloadDigest,
			install: install,
		}
		if _, err := c.submitRecoveryInstallJob(ctx, job); err != nil {
			return c.recordErrorf("recover peras segment install: %w", err)
		}
		if err := c.installSegment(fsperas.ReplayPlan{}, segment, false); err != nil {
			return err
		}
		c.read.mergeCompletions(segment)
		c.metrics.recoveryInstallTotal.Add(1)
	}
	return nil
}

func (c *Runtime) LoadInstalledSegments(ctx context.Context, scope compile.AuthorityScope) error {
	if c == nil || c.catalog == nil || scope.MountKeyID == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := c.scanInstalledSegmentCatalogs(ctx, scope)
	if err != nil {
		return c.recordErrorf("load peras segment catalogs: %w", err)
	}
	for _, record := range records {
		if c.segmentInstalled(record.Root) {
			continue
		}
		segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.Root, record.SegmentPayloadDigest)
		if err != nil {
			return c.recordErrorf("decode peras segment catalog: %w", err)
		}
		if !SegmentWithinScope(segment, scope) {
			continue
		}
		if err := c.installSegment(fsperas.ReplayPlan{}, segment, false); err != nil {
			return err
		}
		// Catalog recovery does not run the install chain (the catalog
		// already exists on disk), so the completion-index update has
		// no chain-side owner here — invoke the helper directly to keep
		// SubmitVisible dedup working after restart.
		c.read.mergeCompletions(segment)
		c.metrics.catalogLoadTotal.Add(1)
	}
	return nil
}

func (c *Runtime) LoadRootSealedSegments(ctx context.Context, scope compile.AuthorityScope) error {
	if c == nil {
		return ErrRuntimeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.seals == nil {
		return c.LoadInstalledSegments(ctx, scope)
	}
	seals, err := c.seals.ListVisibleAuthoritySeals(ctx, scope)
	if err != nil {
		return c.recordErrorf("list rooted peras segment seals: %w", err)
	}
	if len(seals) == 0 {
		return nil
	}
	catalogErr := c.LoadInstalledSegments(ctx, scope)
	if catalogErr != nil && (c.installer == nil || len(c.witnesses) == 0 || !recoverableCatalogLoadError(catalogErr)) {
		return catalogErr
	}
	for _, seal := range seals {
		if !seal.Valid() {
			return c.recordError(fsperas.ErrInvalidPerasSegment)
		}
		c.metrics.rootSealTotal.Add(1)
		if c.segmentInstalled(seal.SegmentRoot) {
			continue
		}
		c.metrics.rootSealMissingTotal.Add(1)
		if err := c.recoverRootSealedSegment(ctx, scope, seal); err != nil {
			return c.recordErrorf("rooted peras segment seal missing installed catalog: %w", err)
		}
	}
	return nil
}

func (c *Runtime) recoverRootSealedSegment(ctx context.Context, scope compile.AuthorityScope, seal rootproto.VisibleAuthoritySeal) error {
	if c == nil || c.installer == nil || len(c.witnesses) == 0 {
		return fsperas.ErrInvalidPerasSegment
	}
	record, found, err := c.collectRootSealedWitnessSegment(ctx, seal)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("epoch=%d root=%x digest=%x: %w", seal.EpochID, seal.SegmentRoot, seal.SegmentPayloadDigest, fsperas.ErrInvalidPerasSegment)
	}
	if err := fsperas.VerifySegmentWitnessRecord(record); err != nil {
		return fmt.Errorf("verify witness record: %w", err)
	}
	segment, err := fsperas.VerifyPerasSegmentPayload(record.SegmentPayload, record.SegmentRoot, record.SegmentPayloadDigest)
	if err != nil {
		return fmt.Errorf("verify witness payload: %w", err)
	}
	sealScope := ScopeFromSeal(seal)
	if !SegmentWithinScope(segment, sealScope) {
		return fmt.Errorf("segment outside root seal scope: %w", fsperas.ErrInvalidPerasSegment)
	}
	if !ScopeEmpty(scope) && !ScopesOverlap(scope, sealScope) {
		return fmt.Errorf("root seal scope outside recovery scope: %w", fsperas.ErrInvalidPerasSegment)
	}
	stats := segment.Stats()
	if record.OperationCount != stats.OperationCount || record.EntryCount != stats.EntryCount {
		return fmt.Errorf("witness stats mismatch: %w", fsperas.ErrInvalidWitnessRecord)
	}
	if seal.OperationCount != 0 && seal.OperationCount != stats.OperationCount {
		return fmt.Errorf("root seal operation count mismatch: %w", fsperas.ErrInvalidPerasSegment)
	}
	if seal.EntryCount != 0 && seal.EntryCount != stats.EntryCount {
		return fmt.Errorf("root seal entry count mismatch: %w", fsperas.ErrInvalidPerasSegment)
	}
	install, err := fsperas.PerasSegmentInstallPlan(segment, false)
	if err != nil {
		return fmt.Errorf("plan rooted segment install: %w", err)
	}
	job := perasFlushJob{
		scope:   scope,
		segment: segment,
		payload: record.SegmentPayload,
		digest:  record.SegmentPayloadDigest,
		install: install,
	}
	if _, err := c.submitRootRecoveryInstallJob(ctx, job); err != nil {
		return err
	}
	if err := c.installSegment(fsperas.ReplayPlan{}, segment, false); err != nil {
		return err
	}
	c.read.mergeCompletions(segment)
	c.metrics.recoveryInstallTotal.Add(1)
	return nil
}

func recoverableCatalogLoadError(err error) bool {
	return nokverrors.Retryable(err) || nokverrors.IsKind(err, nokverrors.KindRetryExhausted)
}

func (c *Runtime) submitRootRecoveryInstallJob(ctx context.Context, job perasFlushJob) (InstallCursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	installCtx, cancel := context.WithTimeout(ctx, defaultPerasRootRecoveryInstallTimeout)
	cursor, err := c.submitRecoveryInstallJob(installCtx, job)
	cancel()
	if err == nil {
		return cursor, nil
	}
	if ctx.Err() != nil {
		return InstallCursor{}, ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || recoverableRecoveryInstallError(err) {
		c.startRecoveryInstallRetry(job)
		return InstallCursor{}, nil
	}
	return InstallCursor{}, err
}

func (c *Runtime) submitRecoveryInstallJob(ctx context.Context, job perasFlushJob) (InstallCursor, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for attempt := 0; ; attempt++ {
		cursor, err := c.submitInstallJob(ctx, job)
		if err == nil {
			return cursor, nil
		}
		if !recoverableRecoveryInstallError(err) {
			return InstallCursor{}, err
		}
		c.recordInstallRetry(err)
		if !sleepContext(ctx, perasSegmentInstallRetryDelay(err, attempt)) {
			return InstallCursor{}, ctx.Err()
		}
	}
}

func recoverableRecoveryInstallError(err error) bool {
	return nokverrors.Retryable(err) || nokverrors.IsKind(err, nokverrors.KindRetryExhausted)
}

func (c *Runtime) startRecoveryInstallRetry(job perasFlushJob) {
	if c == nil || c.closer == nil {
		return
	}
	c.closer.Add(1)
	go func() {
		defer c.closer.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-c.closer.Closed():
				cancel()
			case <-ctx.Done():
			}
		}()
		if _, err := c.submitRecoveryInstallJob(ctx, job); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrRuntimeClosed) {
			_ = c.recordErrorf("background recover peras segment install: %w", err)
		}
	}()
}

func (c *Runtime) collectRootSealedWitnessSegment(ctx context.Context, seal rootproto.VisibleAuthoritySeal) (fsperas.SegmentWitnessRecord, bool, error) {
	ref := fsperas.WitnessSegmentRef{
		EpochID:              seal.EpochID,
		SegmentRoot:          seal.SegmentRoot,
		SegmentPayloadDigest: seal.SegmentPayloadDigest,
	}
	record, found, err := c.collectWitnessSegment(ctx, ref)
	if err != nil || found {
		return record, found, err
	}
	records, err := c.collectWitnessSegments(ctx, seal.EpochID)
	if err != nil {
		return fsperas.SegmentWitnessRecord{}, false, err
	}
	for _, candidate := range records {
		if !witnessRecordMatchesRootSeal(candidate, seal) {
			continue
		}
		if !found || len(candidate.SegmentPayload) > len(record.SegmentPayload) {
			record = candidate
			found = true
		}
	}
	return record, found, nil
}

func witnessRecordMatchesRootSeal(record fsperas.SegmentWitnessRecord, seal rootproto.VisibleAuthoritySeal) bool {
	return record.EpochID == seal.EpochID &&
		record.SegmentRoot == seal.SegmentRoot &&
		record.SegmentPayloadDigest == seal.SegmentPayloadDigest
}

func (c *Runtime) scanInstalledSegmentCatalogs(ctx context.Context, scope compile.AuthorityScope) ([]fsperas.SegmentCatalogRecord, error) {
	buckets := CatalogBuckets(scope)
	if len(buckets) == 0 {
		return nil, nil
	}
	records := make([]fsperas.SegmentCatalogRecord, 0)
	seen := make(map[[32]byte]struct{})
	for _, bucket := range buckets {
		prefix, err := layout.EncodeSegmentCatalogIndexPrefix(scope.MountKeyID, bucket)
		if err != nil {
			return nil, err
		}
		next := cloneBytes(prefix)
		for {
			kvs, err := c.catalog.Scan(ctx, next, defaultPerasSegmentCatalogScanLimit, 0)
			if err != nil {
				return nil, err
			}
			if len(kvs) == 0 {
				break
			}
			exhausted := false
			for _, kv := range kvs {
				if !bytes.HasPrefix(kv.Key, prefix) {
					exhausted = true
					break
				}
				index, err := fsperas.DecodePerasSegmentCatalogIndexRecord(kv.Value)
				if err != nil {
					return nil, err
				}
				parts, ok := layout.InspectKey(kv.Key)
				if !ok || parts.Kind != layout.KeyKindSegment || parts.SegmentRecord != layout.SegmentRecordIndex || parts.SegmentRoot != index.Root {
					return nil, fsperas.ErrInvalidPerasSegment
				}
				if _, ok := seen[index.Root]; ok {
					next = scanAfterKey(kv.Key)
					continue
				}
				record, err := c.loadInstalledSegmentObject(ctx, index)
				if err != nil {
					return nil, err
				}
				seen[index.Root] = struct{}{}
				records = append(records, record)
				next = scanAfterKey(kv.Key)
			}
			if exhausted || len(kvs) < defaultPerasSegmentCatalogScanLimit {
				break
			}
		}
	}
	slices.SortFunc(records, func(a, b fsperas.SegmentCatalogRecord) int {
		if a.InstallVersion < b.InstallVersion {
			return -1
		}
		if a.InstallVersion > b.InstallVersion {
			return 1
		}
		return bytes.Compare(a.Root[:], b.Root[:])
	})
	return records, nil
}

func (c *Runtime) loadInstalledSegmentObject(ctx context.Context, index fsperas.SegmentCatalogIndexRecord) (fsperas.SegmentCatalogRecord, error) {
	return c.loadInstalledSegmentObjectAtVersion(ctx, index, 0)
}

func (c *Runtime) loadInstalledSegmentObjectAtVersion(ctx context.Context, index fsperas.SegmentCatalogIndexRecord, version uint64) (fsperas.SegmentCatalogRecord, error) {
	kvs, err := c.catalog.Scan(ctx, index.ObjectKey, 1, version)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, err
	}
	if len(kvs) == 0 || !bytes.Equal(kvs[0].Key, index.ObjectKey) {
		return fsperas.SegmentCatalogRecord{}, fsperas.ErrInvalidPerasSegment
	}
	record, err := fsperas.DecodePerasSegmentCatalogRecord(kvs[0].Value)
	if err != nil {
		return fsperas.SegmentCatalogRecord{}, err
	}
	if record.Root != index.Root ||
		record.InstallVersion != index.InstallVersion ||
		record.SegmentPayloadDigest != index.SegmentPayloadDigest ||
		record.SegmentPayloadSize != index.SegmentPayloadSize {
		return fsperas.SegmentCatalogRecord{}, fsperas.ErrInvalidPerasSegment
	}
	return record, nil
}

func scanAfterKey(key []byte) []byte {
	next := make([]byte, 0, len(key)+1)
	next = append(next, key...)
	return append(next, 0)
}
