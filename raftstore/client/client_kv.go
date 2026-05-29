// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	errorpb "github.com/feichai0017/NoKV/pb/error"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"

	"google.golang.org/protobuf/proto"
)

type regionBatch struct {
	region regionSnapshot
	keys   [][]byte
	ids    []string
}

// Get issues a StoreKV Get RPC for the provided key/version. It retries on region errors.
func (c *Client) Get(ctx context.Context, key []byte, version uint64) (*kvrpcpb.GetResponse, error) {
	return c.GetWithOptions(ctx, key, version, DefaultReadOptions())
}

// GetWithOptions issues a StoreKV Get with explicit read consistency and
// routing preference. Follower-prefer reads fall back to the leader when the
// follower cannot satisfy the requested consistency budget.
func (c *Client) GetWithOptions(ctx context.Context, key []byte, version uint64, opts ReadOptions) (*kvrpcpb.GetResponse, error) {
	opts = normalizeReadOptions(opts)
	var lastErr error
	var lastKeyErr *kvrpcpb.KeyError
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callGetWithFallback(ctx, region, key, version, opts)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		if keyErr := resp.GetError(); keyErr != nil {
			if locked := keyErr.GetLocked(); locked != nil {
				lastKeyErr = keyErr
				resolved, err := c.resolveReadLock(ctx, locked, version)
				if err != nil && !errors.Is(err, errReadLockStillLive) {
					return nil, err
				}
				if !resolved {
					if err := c.waitRetry(ctx, attempt, retryLockResolve); err != nil {
						return nil, err
					}
				}
				continue
			}
			return nil, txnKeyError(keyErr)
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if lastKeyErr != nil {
		return nil, txnKeyError(lastKeyErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "kv get", Key: append([]byte(nil), key...)}
}

// BatchGet fetches multiple keys using the same snapshot version. Keys are
// grouped by region so that each group shares a single BatchGet round-trip
// and read index.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string]*kvrpcpb.GetResponse, error) {
	return c.BatchGetWithOptions(ctx, keys, version, DefaultReadOptions())
}

// BatchGetWithOptions fetches multiple keys with explicit read options.
func (c *Client) BatchGetWithOptions(ctx context.Context, keys [][]byte, version uint64, opts ReadOptions) (map[string]*kvrpcpb.GetResponse, error) {
	opts = normalizeReadOptions(opts)
	results := make(map[string]*kvrpcpb.GetResponse, len(keys))
	if len(keys) == 0 {
		return results, nil
	}
	pending := make(map[string][]byte, len(keys))
	for _, key := range keys {
		keyCopy := append([]byte(nil), key...)
		pending[string(keyCopy)] = keyCopy
	}
	var lastErr error
	var lastKeyErr *kvrpcpb.KeyError
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups := make(map[uint64]*regionBatch)
		for keyID, key := range pending {
			region, err := c.routeKeyWithRetry(ctx, key)
			if err != nil {
				return nil, err
			}
			regionID := region.desc.RegionID
			group := groups[regionID]
			if group == nil {
				group = &regionBatch{region: region}
				groups[regionID] = group
			}
			group.keys = append(group.keys, key)
			group.ids = append(group.ids, keyID)
		}
		var completed []string
		for regionID, group := range groups {
			resp, regionErr, err := c.callBatchGetWithFallback(ctx, group.region, group.keys, version, opts)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					continue
				}
				return nil, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(regionID, regionErr)
				if lastErr != nil {
					return nil, lastErr
				}
				continue
			}
			if err := c.collectBatchGetResponses(resp, group, results, &completed, &lastKeyErr, version, ctx); err != nil {
				if errors.Is(err, errReadLockStillLive) {
					lastErr = err
					continue
				}
				return nil, err
			}
		}
		for _, keyID := range completed {
			delete(pending, keyID)
		}
		if len(pending) > 0 {
			kind := retryRegionError
			if IsRouteUnavailable(lastErr) {
				kind = retryRouteUnavailable
			} else if isTransportUnavailable(lastErr) {
				kind = retryTransportUnavailable
			} else if lastKeyErr != nil {
				kind = retryLockResolve
			}
			if err := c.waitRetry(ctx, attempt, kind); err != nil {
				return nil, err
			}
		}
	}
	if len(pending) > 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		if lastKeyErr != nil {
			return nil, txnKeyError(lastKeyErr)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, &RetryExhaustedError{Operation: "kv batch get"}
	}
	return results, nil
}

func (c *Client) collectBatchGetResponses(resp *kvrpcpb.BatchGetResponse, group *regionBatch, results map[string]*kvrpcpb.GetResponse, completed *[]string, lastKeyErr **kvrpcpb.KeyError, version uint64, ctx context.Context) error {
	responses := resp.GetResponses()
	for i, keyID := range group.ids {
		var getResp *kvrpcpb.GetResponse
		if i < len(responses) && responses[i] != nil {
			getResp = responses[i]
		} else {
			getResp = &kvrpcpb.GetResponse{NotFound: true}
		}
		if keyErr := getResp.GetError(); keyErr != nil {
			if locked := keyErr.GetLocked(); locked != nil {
				*lastKeyErr = keyErr
				resolved, err := c.resolveReadLock(ctx, locked, version)
				if err != nil && !errors.Is(err, errReadLockStillLive) {
					return err
				}
				if !resolved {
					return errReadLockStillLive
				}
				continue
			}
			return txnKeyError(keyErr)
		}
		results[keyID] = getResp
		*completed = append(*completed, keyID)
	}
	return nil
}

func (c *Client) callGet(ctx context.Context, region regionSnapshot, key []byte, version uint64, opts ReadOptions) (*kvrpcpb.GetResponse, *errorpb.RegionError, error) {
	targetStoreID := readTargetStore(region, opts)
	cl, err := c.storeClient(ctx, targetStoreID)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildReadContext(region, targetStoreID, opts)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cl.Get(ctx, &kvrpcpb.KvGetRequest{
		Context: header,
		Request: &kvrpcpb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		},
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) callGetWithFallback(ctx context.Context, region regionSnapshot, key []byte, version uint64, opts ReadOptions) (*kvrpcpb.GetResponse, *errorpb.RegionError, error) {
	resp, regionErr, err := c.callGet(ctx, region, key, version, opts)
	if err == nil && regionErr == nil {
		return resp, nil, nil
	}
	if !followerPrefer(opts) {
		return resp, regionErr, err
	}
	if err != nil && !isTransportUnavailable(err) {
		return nil, nil, err
	}
	if regionErr != nil && regionErr.GetStaleCommand() == nil {
		return resp, regionErr, nil
	}
	return c.callGet(ctx, region, key, version, leaderReadOptions(opts))
}

func (c *Client) callBatchGet(ctx context.Context, region regionSnapshot, keys [][]byte, version uint64, opts ReadOptions) (*kvrpcpb.BatchGetResponse, *errorpb.RegionError, error) {
	if len(keys) == 0 {
		return &kvrpcpb.BatchGetResponse{}, nil, nil
	}
	targetStoreID := readTargetStore(region, opts)
	cl, err := c.storeClient(ctx, targetStoreID)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildReadContext(region, targetStoreID, opts)
	if err != nil {
		return nil, nil, err
	}
	request := &kvrpcpb.BatchGetRequest{Requests: make([]*kvrpcpb.GetRequest, 0, len(keys))}
	for _, key := range keys {
		request.Requests = append(request.Requests, &kvrpcpb.GetRequest{
			Key:     append([]byte(nil), key...),
			Version: version,
		})
	}
	resp, err := cl.BatchGet(ctx, &kvrpcpb.KvBatchGetRequest{Context: header, Request: request})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	out := resp.GetResponse()
	if out == nil {
		out = &kvrpcpb.BatchGetResponse{}
	}
	return out, resp.GetRegionError(), nil
}

func (c *Client) callBatchGetWithFallback(ctx context.Context, region regionSnapshot, keys [][]byte, version uint64, opts ReadOptions) (*kvrpcpb.BatchGetResponse, *errorpb.RegionError, error) {
	resp, regionErr, err := c.callBatchGet(ctx, region, keys, version, opts)
	if err == nil && regionErr == nil {
		return resp, nil, nil
	}
	if !followerPrefer(opts) {
		return resp, regionErr, err
	}
	if err != nil && !isTransportUnavailable(err) {
		return nil, nil, err
	}
	if regionErr != nil && regionErr.GetStaleCommand() == nil {
		return resp, regionErr, nil
	}
	return c.callBatchGet(ctx, region, keys, version, leaderReadOptions(opts))
}

// Scan issues a forward StoreKV Scan RPC starting at startKey, reading up to limit keys.
func (c *Client) Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]*kvrpcpb.KV, error) {
	return c.ScanWithOptions(ctx, startKey, limit, version, DefaultReadOptions())
}

// ScanWithOptions issues a scan with explicit read consistency and routing preference.
func (c *Client) ScanWithOptions(ctx context.Context, startKey []byte, limit uint32, version uint64, opts ReadOptions) ([]*kvrpcpb.KV, error) {
	opts = normalizeReadOptions(opts)
	if limit == 0 {
		return nil, errInvalidScanLimit
	}
	collected := make([]*kvrpcpb.KV, 0, limit)
	currentKey := append([]byte(nil), startKey...)
	remaining := limit
	lockAttempts := 0
	lastLock := ""
	retryKey := append([]byte(nil), currentKey...)
	transportAttempts := 0
	regionAttempts := 0
	var lastKeyErr *kvrpcpb.KeyError
	resetRetryBudget := func() {
		if bytesCompare(retryKey, currentKey) == 0 {
			return
		}
		retryKey = append(retryKey[:0], currentKey...)
		transportAttempts = 0
		regionAttempts = 0
	}
	waitScanRetry := func(attempts *int, kind retryKind, exhausted error) error {
		*attempts = *attempts + 1
		if *attempts >= c.retry.MaxAttempts {
			if exhausted != nil {
				return exhausted
			}
			return &RetryExhaustedError{Operation: "scan", Key: append([]byte(nil), currentKey...)}
		}
		return c.waitRetry(ctx, *attempts-1, kind)
	}
	for remaining > 0 {
		resetRetryBudget()
		region, err := c.routeKeyWithRetry(ctx, currentKey)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.callScanWithFallback(ctx, region, currentKey, remaining, version, opts)
		if err != nil {
			if isTransportUnavailable(err) {
				if err := waitScanRetry(&transportAttempts, retryTransportUnavailable, err); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			if err := c.handleRegionError(region.desc.RegionID, regionErr); err != nil {
				return nil, err
			}
			retryErr := &RetryExhaustedError{
				Operation: "scan",
				RegionID:  region.desc.RegionID,
				Key:       append([]byte(nil), currentKey...),
				Detail:    "region error retry budget exhausted",
			}
			if err := waitScanRetry(&regionAttempts, retryRegionError, retryErr); err != nil {
				return nil, err
			}
			continue
		}
		if keyErr := resp.GetError(); keyErr != nil {
			if locked := keyErr.GetLocked(); locked != nil {
				lastKeyErr = keyErr
				lockID := readLockFingerprint(locked)
				if lockID != lastLock {
					lastLock = lockID
					lockAttempts = 0
				}
				if lockAttempts >= c.retry.MaxAttempts {
					return nil, txnKeyError(lastKeyErr)
				}
				resolved, err := c.resolveReadLock(ctx, locked, version)
				if err != nil && !errors.Is(err, errReadLockStillLive) {
					return nil, err
				}
				if !resolved {
					if err := c.waitRetry(ctx, lockAttempts, retryLockResolve); err != nil {
						return nil, err
					}
				}
				lockAttempts++
				continue
			}
			return nil, txnKeyError(keyErr)
		}
		kvs := resp.GetKvs()
		collected = append(collected, kvs...)
		if len(kvs) == 0 {
			endKey := region.desc.EndKey
			if len(endKey) == 0 {
				break
			}
			currentKey = append([]byte(nil), endKey...)
			continue
		}
		remaining -= uint32(len(kvs))
		if remaining == 0 {
			break
		}
		endKey := region.desc.EndKey
		nextKey := incrementKey(kvs[len(kvs)-1].GetKey())
		if len(endKey) > 0 && bytesCompare(nextKey, endKey) >= 0 {
			currentKey = append([]byte(nil), endKey...)
			continue
		}
		currentKey = nextKey
	}
	return collected, nil
}

func readLockFingerprint(locked *kvrpcpb.Locked) string {
	return string(locked.GetKey()) + "\x00" + fmt.Sprint(locked.GetLockVersion())
}

func (c *Client) resolveReadLock(ctx context.Context, locked *kvrpcpb.Locked, readTs uint64) (bool, error) {
	if locked == nil {
		return false, nil
	}
	lockKey := locked.GetKey()
	if len(lockKey) == 0 || len(locked.GetPrimaryLock()) == 0 || locked.GetLockVersion() == 0 {
		return false, txnKeyError(&kvrpcpb.KeyError{Locked: locked})
	}
	statusResp, err := c.CheckTxnStatus(ctx, locked.GetPrimaryLock(), locked.GetLockVersion(), readTs, 0)
	if err != nil {
		return false, err
	}
	if statusResp == nil {
		return false, &ProtocolError{Operation: "resolve read lock", Detail: fmt.Sprintf("nil check txn status response for primary %q", locked.GetPrimaryLock())}
	}
	if keyErr := statusResp.GetError(); keyErr != nil {
		return false, txnKeyError(keyErr)
	}
	commitVersion := statusResp.GetCommitVersion()
	switch {
	case commitVersion > 0:
		_, err = c.ResolveLocks(ctx, locked.GetLockVersion(), commitVersion, [][]byte{lockKey})
		return err == nil, err
	case statusResp.GetAction() == kvrpcpb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback ||
		statusResp.GetAction() == kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback:
		_, err = c.ResolveLocks(ctx, locked.GetLockVersion(), 0, [][]byte{lockKey})
		return err == nil, err
	default:
		return false, errReadLockStillLive
	}
}

func (c *Client) callScan(ctx context.Context, region regionSnapshot, startKey []byte, limit uint32, version uint64, opts ReadOptions) (*kvrpcpb.ScanResponse, *errorpb.RegionError, error) {
	targetStoreID := readTargetStore(region, opts)
	cl, err := c.storeClient(ctx, targetStoreID)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildReadContext(region, targetStoreID, opts)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cl.Scan(ctx, &kvrpcpb.KvScanRequest{
		Context: header,
		Request: &kvrpcpb.ScanRequest{
			StartKey:     append([]byte(nil), startKey...),
			Limit:        limit,
			Version:      version,
			IncludeStart: true,
		},
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) callScanWithFallback(ctx context.Context, region regionSnapshot, startKey []byte, limit uint32, version uint64, opts ReadOptions) (*kvrpcpb.ScanResponse, *errorpb.RegionError, error) {
	resp, regionErr, err := c.callScan(ctx, region, startKey, limit, version, opts)
	if err == nil && regionErr == nil {
		return resp, nil, nil
	}
	if !followerPrefer(opts) {
		return resp, regionErr, err
	}
	if err != nil && !isTransportUnavailable(err) {
		return nil, nil, err
	}
	if regionErr != nil && regionErr.GetStaleCommand() == nil {
		return resp, regionErr, nil
	}
	return c.callScan(ctx, region, startKey, limit, version, leaderReadOptions(opts))
}

func leaderReadOptions(opts ReadOptions) ReadOptions {
	opts = normalizeReadOptions(opts)
	if opts.Consistency == kvrpcpb.ReadConsistency_READ_CONSISTENCY_BOUNDED_STALE {
		opts.Consistency = kvrpcpb.ReadConsistency_READ_CONSISTENCY_STRONG
		opts.MaxStaleReadIndex = 0
		opts.MaxStaleReadMS = 0
	}
	opts.Preference = kvrpcpb.ReadPreference_READ_PREFERENCE_LEADER_ONLY
	return opts
}

func readTargetStore(region regionSnapshot, opts ReadOptions) uint64 {
	opts = normalizeReadOptions(opts)
	if followerPrefer(opts) {
		if follower := followerStoreID(region); follower != 0 {
			return follower
		}
	}
	if region.leader != 0 {
		return region.leader
	}
	return defaultLeaderStoreID(region.desc)
}

// Mutate wraps TwoPhaseCommit with a ready-made mutation slice. The caller must
// ensure the primary key is part of the mutation set.
func (c *Client) Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	_, err := c.mutateWithCommitTimestamp(ctx, primary, mutations, startVersion, lockTTL, func(context.Context) (uint64, error) {
		return commitVersion, nil
	})
	return err
}

// MutateWithCommitTimestamp runs a 2PC mutation and obtains commit_ts after all
// prewrites have reached Raft. This is the strict Percolator timestamp boundary:
// readers may push MinCommitTs while locks are live, so fsmeta uses this path to
// avoid exhausting logical-operation retries under read/write contention.
func (c *Client) MutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error) {
	if allocateCommitVersion == nil {
		return 0, &ProtocolError{Operation: "mutate", Detail: "commit timestamp allocator required"}
	}
	return c.mutateWithCommitTimestamp(ctx, primary, mutations, startVersion, lockTTL, allocateCommitVersion)
}

func (c *Client) mutateWithCommitTimestamp(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error) {
	if len(primary) == 0 {
		return 0, &ProtocolError{Operation: "mutate", Detail: "primary key required"}
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cleaned = append(cleaned, cloneMutation(mut))
	}
	if len(cleaned) == 0 {
		return 0, nil
	}
	if !mutationHasPrimary(cleaned, primary) {
		return 0, &ProtocolError{Operation: "mutate", Detail: fmt.Sprintf("primary key %q not present in mutations", primary)}
	}
	return c.twoPhaseCommit(ctx, append([]byte(nil), primary...), cleaned, startVersion, lockTTL, allocateCommitVersion)
}

// TryAtomicMutate attempts to materialize mutations as one region-local 1PC
// Raft command. It returns handled=false when routing or local storage
// atomicity cannot be proven, allowing callers to fall back to regular 2PC.
func (c *Client) TryAtomicMutate(ctx context.Context, primary []byte, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (bool, error) {
	if len(primary) == 0 {
		return false, &ProtocolError{Operation: "atomic mutate", Detail: "primary key required"}
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cleaned = append(cleaned, cloneMutation(mut))
	}
	if len(cleaned) == 0 {
		return true, nil
	}
	preds := cloneAtomicPredicates(predicates)
	if !atomicMutateHasPrimary(cleaned, preds, primary) {
		return false, &ProtocolError{Operation: "atomic mutate", Detail: fmt.Sprintf("primary key %q not present in mutations or predicates", primary)}
	}
	var lastErr error
	ambiguous := false
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		groups, err := c.groupAtomicMutateByRoute(ctx, cleaned, preds)
		if err != nil {
			return false, err
		}
		if len(groups) != 1 {
			c.atomicRouteMultiTotal.Add(1)
			if ambiguous {
				return true, &RetryExhaustedError{Operation: "atomic mutate"}
			}
			return false, nil
		}
		c.atomicRouteSingleTotal.Add(1)
		var group *mutationRouteBatch
		for _, candidate := range groups {
			group = candidate
		}
		resp, regionErr, err := c.tryAtomicMutateRegionOnce(ctx, group.region, preds, cleaned, startVersion, commitVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				ambiguous = true
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return true, err
				}
				continue
			}
			return true, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
			if lastErr != nil {
				return true, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return true, err
			}
			continue
		}
		if resp.GetFallbackToTwoPhaseCommit() {
			c.atomicBackendFallbackTotal.Add(1)
			return false, nil
		}
		if err := txnKeyError(resp.GetError()); err != nil {
			return true, err
		}
		c.atomicSuccessTotal.Add(1)
		return true, nil
	}
	if lastErr != nil {
		return true, lastErr
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	return true, &RetryExhaustedError{Operation: "atomic mutate"}
}

func (c *Client) tryAtomicMutateRegionOnce(ctx context.Context, region regionSnapshot, predicates []*kvrpcpb.AtomicPredicate, mutations []*kvrpcpb.Mutation, startVersion, commitVersion uint64) (*kvrpcpb.TryAtomicMutateResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvTryAtomicMutateRequest{
		Context: header,
		Request: &kvrpcpb.TryAtomicMutateRequest{
			Predicates:    cloneAtomicPredicates(predicates),
			Mutations:     cloneMutations(mutations),
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := cl.TryAtomicMutate(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("atomic mutate", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("atomic mutate", region.desc.RegionID, "missing atomic mutate payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) InstallPreparedMVCCEntries(ctx context.Context, routingKey []byte, req *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, error) {
	if len(routingKey) == 0 {
		return nil, &ProtocolError{Operation: "prepared mvcc install", Detail: "routing key required"}
	}
	cleaned := cloneInstallPreparedMVCCEntriesRequest(req)
	if cleaned == nil {
		return nil, &ProtocolError{Operation: "prepared mvcc install", Detail: "request required"}
	}
	cleaned.RoutingKey = append(cleaned.RoutingKey[:0], routingKey...)
	var lastErr error
	var lastRetryDetail string
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, routingKey)
		if err != nil {
			return nil, err
		}
		resp, regionErr, err := c.installPreparedMVCCEntriesRegionOnce(ctx, region, cleaned)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			lastRetryDetail = fmt.Sprintf("region %d returned %v", region.desc.RegionID, regionErr)
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		if err := txnKeyError(resp.GetError()); err != nil {
			return nil, err
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{
		Operation: "prepared mvcc install",
		Key:       append([]byte(nil), routingKey...),
		Detail:    lastRetryDetail,
	}
}

func (c *Client) installPreparedMVCCEntriesRegionOnce(ctx context.Context, region regionSnapshot, req *kvrpcpb.InstallPreparedMVCCEntriesRequest) (*kvrpcpb.InstallPreparedMVCCEntriesResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	resp, err := cl.InstallPreparedMVCCEntries(ctx, &kvrpcpb.KvInstallPreparedMVCCEntriesRequest{
		Context: header,
		Request: req,
	})
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("prepared mvcc install", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("prepared mvcc install", region.desc.RegionID, "missing install payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// Put performs a single-key Put using the two-phase commit path.
func (c *Client) Put(ctx context.Context, key, value []byte, startVersion, commitVersion, lockTTL uint64) error {
	mut := &kvrpcpb.Mutation{
		Op:    kvrpcpb.Mutation_Put,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	return c.Mutate(ctx, key, []*kvrpcpb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// Delete removes a key using a two-phase commit delete mutation.
func (c *Client) Delete(ctx context.Context, key []byte, startVersion, commitVersion, lockTTL uint64) error {
	mut := &kvrpcpb.Mutation{
		Op:  kvrpcpb.Mutation_Delete,
		Key: append([]byte(nil), key...),
	}
	return c.Mutate(ctx, key, []*kvrpcpb.Mutation{mut}, startVersion, commitVersion, lockTTL)
}

// TwoPhaseCommit runs Prewrite followed by Commit across the supplied mutations.
func (c *Client) TwoPhaseCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error {
	_, err := c.twoPhaseCommit(ctx, primary, mutations, startVersion, lockTTL, func(context.Context) (uint64, error) {
		return commitVersion, nil
	})
	return err
}

func (c *Client) twoPhaseCommit(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, lockTTL uint64, allocateCommitVersion func(context.Context) (uint64, error)) (uint64, error) {
	if len(mutations) == 0 {
		return 0, nil
	}
	cleaned := make([]*kvrpcpb.Mutation, 0, len(mutations))
	var primaryFound bool
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		cloned := cloneMutation(mut)
		if !primaryFound && bytesCompare(cloned.GetKey(), primary) == 0 {
			primaryFound = true
		}
		cleaned = append(cleaned, cloned)
	}
	if !primaryFound {
		return 0, &ProtocolError{Operation: "two phase commit", Detail: fmt.Sprintf("primary key %q missing from mutations", primary)}
	}
	prewritten, secondaryMutations, err := c.prewritePrimaryRegionMutations(ctx, primary, startVersion, lockTTL, cleaned)
	if err != nil {
		return 0, err
	}
	// Read-side lock resolution is allowed to roll back an expired primary
	// lock. Keep the primary alive while secondary prewrites and commit RPCs
	// wait behind raft/store work, otherwise a live long transaction can lose
	// its own lock and surface as retryable "lock not found".
	stopHeartbeat := c.startTxnHeartbeat(ctx, primary, startVersion, lockTTL)
	defer stopHeartbeat()
	if len(secondaryMutations) > 0 {
		secondaryPrewritten, err := c.prewriteMutationsByRoute(ctx, primary, startVersion, lockTTL, secondaryMutations)
		mergePrewritten(prewritten, secondaryPrewritten)
		if err != nil {
			if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
				return 0, errors.Join(err, fmt.Errorf("client: rollback after prewrite failure: %w", rollbackErr))
			}
			return 0, err
		}
	}
	commitVersion, err := allocateCommitVersion(ctx)
	if err != nil {
		if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
			return 0, errors.Join(err, fmt.Errorf("client: rollback after commit timestamp allocation failure: %w", rollbackErr))
		}
		return 0, err
	}
	if commitVersion <= startVersion {
		err := &ProtocolError{Operation: "two phase commit", Detail: fmt.Sprintf("commit version %d must be greater than start version %d", commitVersion, startVersion)}
		if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
			return 0, errors.Join(err, fmt.Errorf("client: rollback after invalid commit timestamp: %w", rollbackErr))
		}
		return 0, err
	}
	// After commit_ts is allocated, callers that publish external frontiers need
	// to know that timestamp even if a later RPC returns an ambiguous error.
	var secondaryKeys [][]byte
	for attempt := 0; ; attempt++ {
		secondaryKeys, err = c.commitPrimaryRegionKeys(ctx, primary, flattenPrewritten(prewritten), startVersion, commitVersion)
		if err == nil {
			break
		}
		if minCommitTs, ok := commitTsExpiredMin(err); ok && attempt+1 < c.retry.MaxAttempts {
			nextCommitVersion, allocErr := allocateCommitVersion(ctx)
			if allocErr != nil {
				if rollbackErr := c.rollbackPrewrites(ctx, prewritten, startVersion); rollbackErr != nil {
					return commitVersion, errors.Join(allocErr, fmt.Errorf("client: rollback after refreshed commit timestamp allocation failure: %w", rollbackErr))
				}
				return commitVersion, allocErr
			}
			// A reader may push MinCommitTs after this client has already
			// allocated commit_ts but before the primary commit is accepted.
			// While the primary lock is still undecided, refreshing commit_ts
			// preserves Percolator's primary-decision rule and avoids leaking a
			// live lock to the higher fsmeta retry loop.
			if nextCommitVersion > commitVersion && nextCommitVersion >= minCommitTs {
				commitVersion = nextCommitVersion
				continue
			}
		}
		if shouldRecoverAfterPrimaryCommitFailure(err) {
			recoveredCommitVersion, recovered, recoverErr := c.recoverPrimaryCommitFailure(ctx, primary, prewritten, startVersion, commitVersion)
			if recoverErr != nil {
				return commitVersion, errors.Join(err, fmt.Errorf("client: recover after primary commit failure: %w", recoverErr))
			}
			if recovered {
				return recoveredCommitVersion, nil
			}
		}
		return commitVersion, err
	}
	if err := c.commitKeysByRoute(ctx, secondaryKeys, startVersion, commitVersion); err != nil {
		if resolveErr := c.resolveCommittedSecondaries(ctx, secondaryKeys, startVersion, commitVersion); resolveErr != nil {
			return commitVersion, errors.Join(err, fmt.Errorf("client: resolve committed secondaries: %w", resolveErr))
		}
		return commitVersion, nil
	}
	return commitVersion, nil
}

type mutationRouteBatch struct {
	region    regionSnapshot
	mutations []*kvrpcpb.Mutation
}

type keyRouteBatch struct {
	region regionSnapshot
	keys   [][]byte
}

func (c *Client) prewritePrimaryRegionMutations(ctx context.Context, primary []byte, startVersion, ttl uint64, mutations []*kvrpcpb.Mutation) (map[uint64][][]byte, []*kvrpcpb.Mutation, error) {
	pending := cloneMutations(mutations)
	prewritten := make(map[uint64][][]byte)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		// Re-route on every retry: a split can move former same-region
		// secondaries away from the primary between attempts.
		groups, err := c.groupMutationsByRoute(ctx, pending)
		if err != nil {
			return prewritten, nil, err
		}
		group := mutationRouteBatchForPrimary(groups, primary)
		if group == nil {
			return prewritten, nil, &ProtocolError{Operation: "prewrite primary region", Detail: fmt.Sprintf("primary key %q missing from routed mutations", primary)}
		}
		resp, regionErr, err := c.prewriteRegionOnce(ctx, group.region, primary, startVersion, ttl, group.mutations)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return prewritten, nil, err
				}
				continue
			}
			return prewritten, nil, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
			if lastErr != nil {
				return prewritten, nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return prewritten, nil, err
			}
			continue
		}
		if resp != nil && len(resp.GetErrors()) > 0 {
			return prewritten, nil, nokverrors.NewTxnKeyError(resp.GetErrors()...)
		}
		prewritten[group.region.desc.RegionID] = append(prewritten[group.region.desc.RegionID], collectKeys(group.mutations)...)
		return prewritten, removeMutationSet(pending, group.mutations), nil
	}
	if lastErr != nil {
		return prewritten, nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return prewritten, nil, err
	}
	return prewritten, nil, &RetryExhaustedError{Operation: "prewrite primary region"}
}

func (c *Client) prewriteMutationsByRoute(ctx context.Context, primary []byte, startVersion, ttl uint64, mutations []*kvrpcpb.Mutation) (map[uint64][][]byte, error) {
	pending := cloneMutations(mutations)
	prewritten := make(map[uint64][][]byte)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupMutationsByRoute(ctx, pending)
		if err != nil {
			return prewritten, err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.prewriteRegionOnce(ctx, group.region, primary, startVersion, ttl, group.mutations)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return prewritten, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return prewritten, lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil && len(resp.GetErrors()) > 0 {
				return prewritten, nokverrors.NewTxnKeyError(resp.GetErrors()...)
			}
			prewritten[group.region.desc.RegionID] = append(prewritten[group.region.desc.RegionID], collectKeys(group.mutations)...)
			pending = removeMutationSet(pending, group.mutations)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return prewritten, err
			}
		}
	}
	if len(pending) == 0 {
		return prewritten, nil
	}
	if lastErr != nil {
		return prewritten, lastErr
	}
	if err := ctx.Err(); err != nil {
		return prewritten, err
	}
	return prewritten, &RetryExhaustedError{Operation: "prewrite"}
}

func (c *Client) rollbackPrewrites(ctx context.Context, prewritten map[uint64][][]byte, startVersion uint64) error {
	return c.rollbackKeysByRoute(ctx, flattenPrewritten(prewritten), startVersion)
}

func (c *Client) resolveCommittedSecondaries(ctx context.Context, keys [][]byte, startVersion, commitVersion uint64) error {
	_, err := c.ResolveLocks(ctx, startVersion, commitVersion, keys)
	return err
}

func shouldRecoverAfterPrimaryCommitFailure(err error) bool {
	_, ok := nokverrors.AsTxnKeyError(err)
	return ok
}

func (c *Client) recoverPrimaryCommitFailure(ctx context.Context, primary []byte, prewritten map[uint64][][]byte, startVersion, commitVersion uint64) (uint64, bool, error) {
	statusResp, err := c.CheckTxnStatus(ctx, primary, startVersion, commitVersion, 0)
	if err != nil {
		return 0, false, err
	}
	if statusResp == nil {
		return 0, false, &ProtocolError{Operation: "recover primary commit", Detail: fmt.Sprintf("nil check txn status response for primary %q", primary)}
	}
	if keyErr := statusResp.GetError(); keyErr != nil {
		return 0, false, txnKeyError(keyErr)
	}
	if resolvedCommitVersion := statusResp.GetCommitVersion(); resolvedCommitVersion != 0 {
		if err := c.resolveCommittedSecondaries(ctx, flattenPrewritten(prewritten), startVersion, resolvedCommitVersion); err != nil {
			return resolvedCommitVersion, false, err
		}
		return resolvedCommitVersion, true, nil
	}
	// A KeyError response from Commit is a deterministic protocol rejection,
	// not an unknown transport outcome. If the primary has no committed
	// decision, clear this start_ts before returning so fsmeta can retry the
	// semantic operation immediately instead of waiting for lock TTL expiry.
	if err := c.rollbackPrewrites(ctx, prewritten, startVersion); err != nil {
		return 0, false, err
	}
	return 0, false, nil
}

func commitTsExpiredMin(err error) (uint64, bool) {
	txnErr, ok := nokverrors.AsTxnKeyError(err)
	if !ok {
		return 0, false
	}
	var minCommitTs uint64
	for _, keyErr := range txnErr.Errors {
		expired := keyErr.GetCommitTsExpired()
		if expired == nil {
			continue
		}
		if expired.MinCommitTs > minCommitTs {
			minCommitTs = expired.MinCommitTs
		}
	}
	return minCommitTs, minCommitTs != 0
}

func txnHeartbeatInterval(lockTTL uint64) time.Duration {
	if lockTTL == 0 {
		return 0
	}
	interval := time.Duration(lockTTL) * time.Millisecond / 3
	if interval <= 0 {
		return time.Millisecond
	}
	return interval
}

// startTxnHeartbeat is best-effort liveness maintenance for a prewritten
// primary. It deliberately does not turn transient heartbeat RPC failures into
// transaction failures: commit remains the authority for the final outcome, and
// heartbeat only prevents healthy long 2PC windows from expiring early.
func (c *Client) startTxnHeartbeat(ctx context.Context, primary []byte, startVersion, lockTTL uint64) func() {
	interval := txnHeartbeatInterval(lockTTL)
	if interval <= 0 {
		return func() {}
	}
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
			}
			resp, err := c.TxnHeartBeat(heartbeatCtx, primary, startVersion, lockTTL)
			if err != nil {
				continue
			}
			if resp.GetError() != nil || resp.GetCommitVersion() != 0 {
				return
			}
			switch resp.GetAction() {
			case kvrpcpb.TxnHeartBeatAction_TxnHeartBeatTTLExpireRollback,
				kvrpcpb.TxnHeartBeatAction_TxnHeartBeatLockNotExistRollback:
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (c *Client) commitPrimaryRegionKeys(ctx context.Context, primary []byte, keys [][]byte, startVersion, commitVersion uint64) ([][]byte, error) {
	pending := cloneKeys(keys)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return nil, err
		}
		group := keyRouteBatchForPrimary(groups, primary)
		if group == nil {
			return nil, &ProtocolError{Operation: "commit primary region", Detail: fmt.Sprintf("primary key %q missing from routed keys", primary)}
		}
		// The first successful commit RPC must contain the primary key. Same-region
		// secondaries are included only to remove an extra round trip; the primary
		// write in this batch remains the Percolator decision record.
		resp, regionErr, err := c.commitRegionOnce(ctx, group.region, group.keys, startVersion, commitVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		if err := txnKeyError(resp.GetError()); err != nil {
			return nil, err
		}
		return removeKeySet(pending, group.keys), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "commit primary region"}
}

func (c *Client) prewriteRegionOnce(ctx context.Context, region regionSnapshot, primary []byte, startVersion, ttl uint64, muts []*kvrpcpb.Mutation) (*kvrpcpb.PrewriteResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvPrewriteRequest{
		Context: header,
		Request: &kvrpcpb.PrewriteRequest{
			Mutations:    cloneMutations(muts),
			PrimaryLock:  append([]byte(nil), primary...),
			StartVersion: startVersion,
			LockTtl:      ttl,
		},
	}
	resp, err := cl.Prewrite(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("prewrite", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("prewrite", region.desc.RegionID, "missing prewrite payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) commitRegion(ctx context.Context, regionID uint64, keys [][]byte, startVersion, commitVersion uint64) error {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, ok := c.regionSnapshot(regionID)
		if !ok {
			return &ProtocolError{Operation: "commit", Detail: fmt.Sprintf("region %d missing from cache", regionID)}
		}
		resp, regionErr, err := c.commitRegionOnce(ctx, region, keys, startVersion, commitVersion)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if regionErr != nil {
			lastErr = c.handleRegionError(regionID, regionErr)
			if lastErr != nil {
				return lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return err
			}
			continue
		}
		if err := txnKeyError(resp.GetError()); err != nil {
			return err
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return &RetryExhaustedError{Operation: "commit", RegionID: regionID}
}

func (c *Client) commitKeysByRoute(ctx context.Context, keys [][]byte, startVersion, commitVersion uint64) error {
	pending := cloneKeys(keys)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.commitRegionOnce(ctx, group.region, group.keys, startVersion, commitVersion)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if err := txnKeyError(resp.GetError()); err != nil {
				return err
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return err
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return &RetryExhaustedError{Operation: "commit"}
}

func (c *Client) commitRegionOnce(ctx context.Context, region regionSnapshot, keys [][]byte, startVersion, commitVersion uint64) (*kvrpcpb.CommitResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvCommitRequest{
		Context: header,
		Request: &kvrpcpb.CommitRequest{
			Keys:          cloneKeys(keys),
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
		},
	}
	resp, err := cl.Commit(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("commit", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("commit", region.desc.RegionID, "missing commit payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func (c *Client) rollbackKeysByRoute(ctx context.Context, keys [][]byte, startVersion uint64) error {
	pending := cloneKeys(keys)
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.batchRollbackRegionOnce(ctx, group.region, group.keys, startVersion)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if err := txnKeyError(resp.GetError()); err != nil {
				return err
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return err
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return &RetryExhaustedError{Operation: "rollback"}
}

func (c *Client) batchRollbackRegionOnce(ctx context.Context, region regionSnapshot, keys [][]byte, startVersion uint64) (*kvrpcpb.BatchRollbackResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvBatchRollbackRequest{
		Context: header,
		Request: &kvrpcpb.BatchRollbackRequest{
			Keys:         cloneKeys(keys),
			StartVersion: startVersion,
		},
	}
	resp, err := cl.BatchRollback(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("rollback", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("rollback", region.desc.RegionID, "missing rollback payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

// CheckTxnStatus inspects the primary lock for a transaction and returns the
// scheduler's decision (rollback, still alive, or already committed).
func (c *Client) CheckTxnStatus(ctx context.Context, primary []byte, lockTs, currentTs, currentTime uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, primary)
		if err != nil {
			return nil, err
		}
		cl, err := c.storeClient(ctx, region.leader)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		header, err := buildContext(region)
		if err != nil {
			return nil, err
		}
		req := &kvrpcpb.KvCheckTxnStatusRequest{
			Context: header,
			Request: &kvrpcpb.CheckTxnStatusRequest{
				PrimaryKey:         append([]byte(nil), primary...),
				LockTs:             lockTs,
				CurrentTs:          currentTs,
				CallerStartTs:      currentTs,
				RollbackIfNotExist: true,
				CurrentTime:        currentTime,
			},
		}
		resp, err := cl.CheckTxnStatus(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if resp == nil {
			return nil, kvPayloadProtocolError("check txn status", region.desc.RegionID, "nil kv response")
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		if resp.GetResponse() == nil {
			return nil, kvPayloadProtocolError("check txn status", region.desc.RegionID, "missing check txn status payload")
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "check txn status"}
}

// TxnHeartBeat extends the primary lock TTL for a live transaction.
func (c *Client) TxnHeartBeat(ctx context.Context, primary []byte, startVersion, ttlExtension uint64) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return c.txnHeartBeatAt(ctx, primary, startVersion, ttlExtension, 0)
}

func (c *Client) txnHeartBeatAt(ctx context.Context, primary []byte, startVersion, ttlExtension, currentTime uint64) (*kvrpcpb.TxnHeartBeatResponse, error) {
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		region, err := c.routeKeyWithRetry(ctx, primary)
		if err != nil {
			return nil, err
		}
		cl, err := c.storeClient(ctx, region.leader)
		if err != nil {
			if isTransportUnavailable(err) {
				lastErr = err
				if err := c.waitRetry(ctx, attempt, retryTransportUnavailable); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		header, err := buildContext(region)
		if err != nil {
			return nil, err
		}
		req := &kvrpcpb.KvTxnHeartBeatRequest{
			Context: header,
			Request: &kvrpcpb.TxnHeartBeatRequest{
				PrimaryKey:   append([]byte(nil), primary...),
				StartVersion: startVersion,
				TtlExtension: ttlExtension,
				CurrentTime:  currentTime,
			},
		}
		resp, err := cl.TxnHeartBeat(ctx, req)
		if err != nil {
			return nil, normalizeRPCError(err)
		}
		if resp == nil {
			return nil, kvPayloadProtocolError("txn heartbeat", region.desc.RegionID, "nil kv response")
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			lastErr = c.handleRegionError(region.desc.RegionID, regionErr)
			if lastErr != nil {
				return nil, lastErr
			}
			if err := c.waitRetry(ctx, attempt, retryRegionError); err != nil {
				return nil, err
			}
			continue
		}
		if resp.GetResponse() == nil {
			return nil, kvPayloadProtocolError("txn heartbeat", region.desc.RegionID, "missing txn heartbeat payload")
		}
		return resp.GetResponse(), nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, &RetryExhaustedError{Operation: "txn heartbeat"}
}

// ResolveLocks attempts to resolve (commit or rollback) the provided keys for
// the given transaction versions. Keys are grouped by region automatically.
func (c *Client) ResolveLocks(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	return c.resolveLocksByRoute(ctx, startVersion, commitVersion, keys)
}

func (c *Client) resolveLocksByRoute(ctx context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	pending := cloneKeys(keys)
	var resolved uint64
	var lastErr error
	for attempt := 0; attempt < c.retry.MaxAttempts && len(pending) > 0; attempt++ {
		groups, err := c.groupKeysByRoute(ctx, pending)
		if err != nil {
			return resolved, err
		}
		var retryKindForAttempt retryKind
		shouldRetry := false
		for _, group := range groups {
			resp, regionErr, err := c.resolveRegionLocksOnce(ctx, group.region, startVersion, commitVersion, group.keys)
			if err != nil {
				if isTransportUnavailable(err) {
					lastErr = err
					retryKindForAttempt = retryTransportUnavailable
					shouldRetry = true
					break
				}
				return resolved, err
			}
			if regionErr != nil {
				lastErr = c.handleRegionError(group.region.desc.RegionID, regionErr)
				if lastErr != nil {
					return resolved, lastErr
				}
				retryKindForAttempt = retryRegionError
				shouldRetry = true
				break
			}
			if resp != nil {
				if keyErr := resp.GetError(); keyErr != nil {
					return resolved, txnKeyError(keyErr)
				}
				resolved += resp.GetResolvedLocks()
			}
			pending = removeKeySet(pending, group.keys)
		}
		if shouldRetry {
			if err := c.waitRetry(ctx, attempt, retryKindForAttempt); err != nil {
				return resolved, err
			}
		}
	}
	if len(pending) == 0 {
		return resolved, nil
	}
	if lastErr != nil {
		return resolved, lastErr
	}
	if err := ctx.Err(); err != nil {
		return resolved, err
	}
	return resolved, &RetryExhaustedError{Operation: "resolve lock"}
}

func (c *Client) resolveRegionLocksOnce(ctx context.Context, region regionSnapshot, startVersion, commitVersion uint64, keys [][]byte) (*kvrpcpb.ResolveLockResponse, *errorpb.RegionError, error) {
	cl, err := c.storeClient(ctx, region.leader)
	if err != nil {
		return nil, nil, err
	}
	header, err := buildContext(region)
	if err != nil {
		return nil, nil, err
	}
	req := &kvrpcpb.KvResolveLockRequest{
		Context: header,
		Request: &kvrpcpb.ResolveLockRequest{
			StartVersion:  startVersion,
			CommitVersion: commitVersion,
			Keys:          cloneKeys(keys),
		},
	}
	resp, err := cl.ResolveLock(ctx, req)
	if err != nil {
		return nil, nil, normalizeRPCError(err)
	}
	if resp == nil {
		return nil, nil, kvPayloadProtocolError("resolve lock", region.desc.RegionID, "nil kv response")
	}
	if resp.GetRegionError() == nil && resp.GetResponse() == nil {
		return nil, nil, kvPayloadProtocolError("resolve lock", region.desc.RegionID, "missing resolve lock payload")
	}
	return resp.GetResponse(), resp.GetRegionError(), nil
}

func kvPayloadProtocolError(operation string, regionID uint64, detail string) error {
	return &ProtocolError{Operation: operation, Detail: fmt.Sprintf("region %d: %s", regionID, detail)}
}

func cloneMutation(mut *kvrpcpb.Mutation) *kvrpcpb.Mutation {
	if mut == nil {
		return nil
	}
	return proto.Clone(mut).(*kvrpcpb.Mutation)
}

func cloneMutations(mutations []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	out := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		out = append(out, cloneMutation(mut))
	}
	return out
}

func cloneAtomicPredicate(pred *kvrpcpb.AtomicPredicate) *kvrpcpb.AtomicPredicate {
	if pred == nil {
		return nil
	}
	return proto.Clone(pred).(*kvrpcpb.AtomicPredicate)
}

func cloneAtomicPredicates(predicates []*kvrpcpb.AtomicPredicate) []*kvrpcpb.AtomicPredicate {
	out := make([]*kvrpcpb.AtomicPredicate, 0, len(predicates))
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		out = append(out, cloneAtomicPredicate(pred))
	}
	return out
}

func cloneInstallPreparedMVCCEntriesRequest(req *kvrpcpb.InstallPreparedMVCCEntriesRequest) *kvrpcpb.InstallPreparedMVCCEntriesRequest {
	if req == nil {
		return nil
	}
	return proto.Clone(req).(*kvrpcpb.InstallPreparedMVCCEntriesRequest)
}

func cloneKeys(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		out[i] = append([]byte(nil), key...)
	}
	return out
}

func collectKeys(muts []*kvrpcpb.Mutation) [][]byte {
	out := make([][]byte, 0, len(muts))
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		out = append(out, append([]byte(nil), mut.GetKey()...))
	}
	return out
}

func mutationHasPrimary(muts []*kvrpcpb.Mutation, primary []byte) bool {
	for _, mut := range muts {
		if mut == nil {
			continue
		}
		if bytesCompare(mut.GetKey(), primary) == 0 {
			return true
		}
	}
	return false
}

func atomicMutateHasPrimary(muts []*kvrpcpb.Mutation, preds []*kvrpcpb.AtomicPredicate, primary []byte) bool {
	if mutationHasPrimary(muts, primary) {
		return true
	}
	for _, pred := range preds {
		if pred == nil {
			continue
		}
		if bytesCompare(pred.GetKey(), primary) == 0 {
			return true
		}
	}
	return false
}

func (c *Client) groupMutationsByRoute(ctx context.Context, mutations []*kvrpcpb.Mutation) (map[uint64]*mutationRouteBatch, error) {
	groups := make(map[uint64]*mutationRouteBatch)
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		region, err := c.routeKeyWithRetry(ctx, mut.GetKey())
		if err != nil {
			return nil, err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &mutationRouteBatch{region: region}
			groups[id] = group
		}
		group.mutations = append(group.mutations, mut)
	}
	return groups, nil
}

func mutationRouteBatchForPrimary(groups map[uint64]*mutationRouteBatch, primary []byte) *mutationRouteBatch {
	for _, group := range groups {
		if group == nil {
			continue
		}
		if mutationHasPrimary(group.mutations, primary) {
			return group
		}
	}
	return nil
}

func (c *Client) groupAtomicMutateByRoute(ctx context.Context, mutations []*kvrpcpb.Mutation, predicates []*kvrpcpb.AtomicPredicate) (map[uint64]*mutationRouteBatch, error) {
	groups := make(map[uint64]*mutationRouteBatch)
	addKey := func(key []byte) error {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &mutationRouteBatch{region: region}
			groups[id] = group
		}
		return nil
	}
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		if err := addKey(mut.GetKey()); err != nil {
			return nil, err
		}
	}
	for _, pred := range predicates {
		if pred == nil {
			continue
		}
		if err := addKey(pred.GetKey()); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

func (c *Client) groupKeysByRoute(ctx context.Context, keys [][]byte) (map[uint64]*keyRouteBatch, error) {
	groups := make(map[uint64]*keyRouteBatch)
	for _, key := range keys {
		region, err := c.routeKeyWithRetry(ctx, key)
		if err != nil {
			return nil, err
		}
		id := region.desc.RegionID
		group := groups[id]
		if group == nil {
			group = &keyRouteBatch{region: region}
			groups[id] = group
		}
		group.keys = append(group.keys, append([]byte(nil), key...))
	}
	return groups, nil
}

// GroupKeysByRoute exposes the current route grouping boundary without exposing
// the mutable route cache. It is intended for commands that can merge multiple
// route markers into one per-region raft proposal.
func (c *Client) GroupKeysByRoute(ctx context.Context, keys [][]byte) ([]RouteKeyGroup, error) {
	groups, err := c.groupKeysByRoute(ctx, keys)
	if err != nil {
		return nil, err
	}
	out := make([]RouteKeyGroup, 0, len(groups))
	for _, group := range groups {
		if group == nil {
			continue
		}
		out = append(out, RouteKeyGroup{
			RegionID:      group.region.desc.RegionID,
			LeaderStoreID: group.region.leader,
			Keys:          cloneKeys(group.keys),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RegionID != out[j].RegionID {
			return out[i].RegionID < out[j].RegionID
		}
		return out[i].LeaderStoreID < out[j].LeaderStoreID
	})
	return out, nil
}

func keyRouteBatchForPrimary(groups map[uint64]*keyRouteBatch, primary []byte) *keyRouteBatch {
	for _, group := range groups {
		if group == nil {
			continue
		}
		for _, key := range group.keys {
			if bytesCompare(key, primary) == 0 {
				return group
			}
		}
	}
	return nil
}

func removeMutationSet(pending, completed []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	done := make(map[*kvrpcpb.Mutation]struct{}, len(completed))
	for _, mut := range completed {
		done[mut] = struct{}{}
	}
	out := pending[:0]
	for _, mut := range pending {
		if _, ok := done[mut]; ok {
			continue
		}
		out = append(out, mut)
	}
	return out
}

func removeKeySet(pending, completed [][]byte) [][]byte {
	done := make(map[string]struct{}, len(completed))
	for _, key := range completed {
		done[string(key)] = struct{}{}
	}
	out := pending[:0]
	for _, key := range pending {
		if _, ok := done[string(key)]; ok {
			continue
		}
		out = append(out, key)
	}
	return out
}

func mergePrewritten(dst, src map[uint64][][]byte) {
	for regionID, keys := range src {
		dst[regionID] = append(dst[regionID], cloneKeys(keys)...)
	}
}

func flattenPrewritten(prewritten map[uint64][][]byte) [][]byte {
	var keys [][]byte
	for _, regionKeys := range prewritten {
		keys = append(keys, cloneKeys(regionKeys)...)
	}
	return keys
}
