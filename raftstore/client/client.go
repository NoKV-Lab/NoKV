// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultMaxMessageBytes sizes StoreKV calls that carry sealed segment payloads.
const DefaultMaxMessageBytes = 64 << 20

// RegionResolver resolves Region metadata for an arbitrary key. A Coordinator client
// implementation should satisfy this interface.
type RegionResolver interface {
	GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
	Close() error
}

// StoreResolver resolves runtime store endpoints from the control plane.
type StoreResolver interface {
	GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error)
}

// Config configures the StoreKV distributed client.
type Config struct {
	Context            context.Context
	RegionResolver     RegionResolver
	StoreResolver      StoreResolver
	RouteLookupTimeout time.Duration
	// StoreRevalidateInterval controls how often cached store endpoints are
	// checked against Coordinator. The default is 30s.
	StoreRevalidateInterval time.Duration
	DialTimeout             time.Duration
	DialOptions             []grpc.DialOption
	Retry                   RetryPolicy
}

// RetryPolicy defines retry budgets and backoff for client-side retries.
type RetryPolicy struct {
	MaxAttempts                 int
	RouteUnavailableBackoff     time.Duration
	TransportUnavailableBackoff time.Duration
	RegionErrorBackoff          time.Duration
	LockResolveBackoff          time.Duration
}

// RouteKeyGroup is a client-side snapshot of keys that currently route to one
// Region leader. Callers use it to coalesce one logical operation into fewer
// raft proposals without owning the route cache internals.
type RouteKeyGroup struct {
	RegionID      uint64
	LeaderStoreID uint64
	Keys          [][]byte
}

// Client provides Region-aware helpers for StoreKV RPCs, including 2PC.
type Client struct {
	mu                         sync.RWMutex
	stores                     map[uint64]*storeConn
	regions                    map[uint64]*regionState
	regionIndex                []regionRange
	regionResolver             RegionResolver
	storeResolver              StoreResolver
	routeDescriptorFloors      []routeDescriptorFloor
	routeLookupTimeout         time.Duration
	storeRevalidateIn          time.Duration
	dialTimeout                time.Duration
	dialOpts                   []grpc.DialOption
	retry                      RetryPolicy
	atomicRouteSingleTotal     atomic.Uint64
	atomicRouteMultiTotal      atomic.Uint64
	atomicBackendFallbackTotal atomic.Uint64
	atomicSuccessTotal         atomic.Uint64
}

// New constructs a Client using the provided configuration.
func New(cfg Config) (*Client, error) {
	if cfg.RegionResolver == nil {
		return nil, errMissingRegionResolver
	}
	if cfg.StoreResolver == nil {
		return nil, errMissingStoreResolver
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 3 * time.Second
	}
	routeLookupTimeout := cfg.RouteLookupTimeout
	if routeLookupTimeout <= 0 {
		routeLookupTimeout = 2 * time.Second
	}
	storeRevalidateIn := cfg.StoreRevalidateInterval
	if storeRevalidateIn <= 0 {
		storeRevalidateIn = 30 * time.Second
	}
	dialOpts := normalizeStoreDialOptions(cfg.DialOptions)
	retry := cfg.Retry
	if retry.MaxAttempts <= 0 {
		retry.MaxAttempts = 5
	}
	retry.RouteUnavailableBackoff = normalizeRetryBackoff(retry.RouteUnavailableBackoff, 20*time.Millisecond)
	retry.TransportUnavailableBackoff = normalizeRetryBackoff(retry.TransportUnavailableBackoff, 10*time.Millisecond)
	retry.RegionErrorBackoff = normalizeRetryBackoff(retry.RegionErrorBackoff, 0)
	retry.LockResolveBackoff = normalizeRetryBackoff(retry.LockResolveBackoff, 10*time.Millisecond)
	return &Client{
		stores:             make(map[uint64]*storeConn),
		regions:            make(map[uint64]*regionState),
		regionResolver:     cfg.RegionResolver,
		storeResolver:      cfg.StoreResolver,
		routeLookupTimeout: routeLookupTimeout,
		storeRevalidateIn:  storeRevalidateIn,
		dialTimeout:        dialTimeout,
		dialOpts:           append([]grpc.DialOption(nil), dialOpts...),
		retry:              retry,
	}, nil
}

func normalizeStoreDialOptions(opts []grpc.DialOption) []grpc.DialOption {
	out := make([]grpc.DialOption, 0, len(opts)+2)
	if len(opts) == 0 {
		out = append(out, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		out = append(out, opts...)
	}
	out = append(out, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(DefaultMaxMessageBytes),
		grpc.MaxCallSendMsgSize(DefaultMaxMessageBytes),
	))
	return out
}

// Close terminates outstanding store connections.
func (c *Client) Close() error {
	var first error
	c.mu.RLock()
	stores := make(map[uint64]*storeConn, len(c.stores))
	maps.Copy(stores, c.stores)
	resolver := c.regionResolver
	c.mu.RUnlock()
	for id, st := range stores {
		if st == nil {
			continue
		}
		if err := st.close(); err != nil && first == nil {
			first = fmt.Errorf("client: close store %d: %w", id, err)
		}
	}
	if resolver != nil {
		if err := resolver.Close(); err != nil && first == nil {
			first = fmt.Errorf("client: close region resolver: %w", err)
		}
	}
	return first
}

// Stats exposes client-side visible-commit admission counters. They are counted per
// route attempt so region churn and retry loops are visible in benchmark runs.
func (c *Client) Stats() map[string]any {
	if c == nil {
		return map[string]any{
			"atomic_route_single_total":     uint64(0),
			"atomic_route_multi_total":      uint64(0),
			"atomic_backend_fallback_total": uint64(0),
			"atomic_success_total":          uint64(0),
		}
	}
	return map[string]any{
		"atomic_route_single_total":     c.atomicRouteSingleTotal.Load(),
		"atomic_route_multi_total":      c.atomicRouteMultiTotal.Load(),
		"atomic_backend_fallback_total": c.atomicBackendFallbackTotal.Load(),
		"atomic_success_total":          c.atomicSuccessTotal.Load(),
	}
}
