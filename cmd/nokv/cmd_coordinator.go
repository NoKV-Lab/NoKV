// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/feichai0017/NoKV/config"
	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	coordserver "github.com/feichai0017/NoKV/coordinator/server"
	"github.com/feichai0017/NoKV/coordinator/tso"
	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc"
)

var coordinatorNotifyContext = signal.NotifyContext
var coordinatorListen = net.Listen

// runCoordinatorCmd launches one coordinator process. NoKV only ships the
// separated-truth-plane topology: coordinator always talks to an external
// 3-peer meta-root cluster via gRPC. Embedded local/replicated root modes
// have been removed.
func runCoordinatorCmd(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:2379", "listen address for coordinator gRPC service")
	idStart := fs.Uint64("id-start", 1, "initial ID allocator value (used only when the meta-root cluster has no allocator state)")
	tsStart := fs.Uint64("ts-start", 1, "initial TSO value (used only when the meta-root cluster has no allocator state)")
	idWindowSize := fs.Uint64("id-window-size", 0, "ID authority window size before coordinator must renew its grant (0 uses default)")
	tsoWindowSize := fs.Uint64("tso-window-size", 0, "TSO authority window size before coordinator must renew its grant (0 uses default)")
	rootRefresh := fs.Duration("root-refresh", 200*time.Millisecond, "refresh interval for rooted coordinator state")
	coordinatorID := fs.String("coordinator-id", "", "stable coordinator owner id for grant-gated control plane (required)")
	grantTTL := fs.Duration("grant-ttl", 10*time.Second, "coordinator grant ttl")
	grantRenewBefore := fs.Duration("grant-renew-before", 3*time.Second, "renew/campaign before grant expiry")
	grantCandidates := fs.String("grant-candidates", "", "comma-separated coordinator ids eligible for duty grants")
	grantDuties := fs.String("grant-duties", "alloc_id,tso,region_lookup", "comma-separated duties eligible on this coordinator")
	shutdownGrace := fs.Duration("shutdown-grace", 0, "maximum time to drain and seal coordinator grant before graceful shutdown (default: grant ttl, or 10s when grant ttl is disabled)")
	configPath := fs.String("config", "", "optional raft configuration file used to resolve coordinator listen address")
	scope := fs.String("scope", "host", "scope for config-resolved coordinator address: host|docker")
	metricsAddr := fs.String("metrics-addr", "", "optional HTTP address to expose /debug/vars expvar endpoint")
	var rootPeerFlags []string
	fs.Func("root-peer", "remote metadata root gRPC peer mapping in the form nodeID=address (repeatable, exactly 3)", func(value string) error {
		value = strings.TrimSpace(value)
		if value == "" {
			return fmt.Errorf("root-peer value cannot be empty")
		}
		rootPeerFlags = append(rootPeerFlags, value)
		return nil
	})
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		return err
	}
	var cfg *config.File
	if strings.TrimSpace(*configPath) != "" {
		scopeNorm := strings.ToLower(strings.TrimSpace(*scope))
		if scopeNorm != "host" && scopeNorm != "docker" {
			return fmt.Errorf("invalid coordinator scope %q (expected host|docker)", *scope)
		}
		loaded, err := config.LoadFile(strings.TrimSpace(*configPath))
		if err != nil {
			return fmt.Errorf("coordinator load config %q: %w", strings.TrimSpace(*configPath), err)
		}
		cfg = loaded
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("coordinator validate config %q: %w", strings.TrimSpace(*configPath), err)
		}
		if !flagPassed(fs, "addr") {
			if resolved := cfg.ResolveCoordinatorAddr(scopeNorm); resolved != "" {
				*addr = resolved
			}
		}
		// Resolve --root-peer from meta_root.peers when not supplied
		// explicitly. This keeps the meta-root cluster address list in one
		// place (the config) instead of duplicated across docker-compose,
		// systemd units, and coord CLI invocations.
		if !flagPassed(fs, "root-peer") && cfg.MetaRoot != nil {
			for id, paddr := range cfg.MetaRootServicePeers(scopeNorm) {
				rootPeerFlags = append(rootPeerFlags, fmt.Sprintf("%d=%s", id, paddr))
			}
		}
	}

	coordinatorIDValue := strings.TrimSpace(*coordinatorID)
	if coordinatorIDValue == "" {
		return fmt.Errorf("coordinator requires --coordinator-id")
	}

	rootPeers, err := parseReplicatedRootPeers(rootPeerFlags)
	if err != nil {
		return err
	}
	if len(rootPeers) != 3 {
		return fmt.Errorf("coordinator requires exactly 3 --root-peer values")
	}

	lis, err := coordinatorListen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("coordinator listen on %s: %w", *addr, err)
	}
	defer func() { _ = lis.Close() }()

	cluster := catalog.NewCluster()
	rootStore, err := rootview.OpenRootRemoteStore(rootview.RemoteRootConfig{
		Targets: rootPeers,
	})
	if err != nil {
		return fmt.Errorf("coordinator open remote metadata root: %w", err)
	}
	defer func() { _ = rootStore.Close() }()
	bootstrap, err := rootview.Bootstrap(rootStore, cluster.PublishRegionDescriptor, *idStart, *tsStart)
	if err != nil {
		return fmt.Errorf("coordinator bootstrap from remote metadata root: %w", err)
	}
	cluster.ReplaceRootSnapshot(bootstrap.Snapshot.RootSnapshot(), bootstrap.Snapshot.RootToken)
	*idStart, *tsStart = bootstrap.IDStart, bootstrap.TSStart
	loadedRegions := bootstrap.LoadedRegions

	ids := idalloc.NewIDAllocator(*idStart)
	tsAlloc := tso.NewAllocator(*tsStart)
	svc := coordserver.NewService(cluster, ids, tsAlloc, rootStore)
	svc.ConfigureAllocatorWindows(*idWindowSize, *tsoWindowSize)
	if cfg != nil && cfg.FSMetaRegionBootstrap != nil {
		boundaries, err := fsmetaBootstrapSplitBoundaries(cfg.FSMetaRegionBootstrap)
		if err != nil {
			return err
		}
		svc.ConfigureSchedulerSplitBoundaries(boundaries)
	}
	parsedDuties, err := parseCoordinatorGrantDuties(*grantDuties)
	if err != nil {
		return err
	}
	svc.ConfigureAuthorityGrantDuties(coordinatorIDValue, splitCommaList(*grantCandidates), parsedDuties, *grantTTL, *grantRenewBefore)
	installCoordinatorExpvar(svc)

	grpcServer := grpc.NewServer()
	coordpb.RegisterCoordinatorServer(grpcServer, svc)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()
	metricsLn, err := startExpvarServer(*metricsAddr)
	if err != nil {
		return fmt.Errorf("start coordinator metrics endpoint: %w", err)
	}
	if metricsLn != nil {
		defer func() { _ = metricsLn.Close() }()
	}

	_, _ = fmt.Fprintf(w, "Coordinator restored %d region(s) from remote metadata root\n", loadedRegions)
	_, _ = fmt.Fprintf(w, "Coordinator allocator starts: id=%d ts=%d\n", *idStart, *tsStart)
	_, _ = fmt.Fprintf(w, "Coordinator grant holder: id=%s ttl=%s renew_before=%s duties=%s candidates=%s\n", coordinatorIDValue, grantTTL.String(), grantRenewBefore.String(), strings.Join(dutyNames(parsedDuties), ","), strings.Join(splitCommaList(*grantCandidates), ","))
	_, _ = fmt.Fprintf(w, "Coordinator service listening on %s\n", lis.Addr().String())
	if metricsLn != nil {
		_, _ = fmt.Fprintf(w, "Coordinator metrics endpoint listening on http://%s/debug/vars\n", metricsLn.Addr().String())
	}

	ctx, cancel := coordinatorNotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go svc.RunGrantLoop(ctx)
	go func() {
		subscription := rootStore.SubscribeTail(rootstorage.TailToken{})
		if subscription == nil {
			return
		}
		for {
			next, err := subscription.Next(ctx, *rootRefresh)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}
			switch next.CatchUpAction() {
			case rootstorage.TailCatchUpRefreshState, rootstorage.TailCatchUpInstallBootstrap:
				if err := svc.ReloadFromStorage(); err != nil {
					continue
				}
				subscription.Acknowledge(next)
			case rootstorage.TailCatchUpAcknowledgeWindow:
				subscription.Acknowledge(next)
			}
		}
	}()

	select {
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	case <-ctx.Done():
		drainBudget := *shutdownGrace
		if drainBudget <= 0 {
			drainBudget = *grantTTL
		}
		if drainBudget <= 0 {
			drainBudget = 10 * time.Second
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), drainBudget)
		if err := svc.DrainAndSealGrant(shutdownCtx); err != nil {
			_, _ = fmt.Fprintf(w, "Coordinator drain/seal grant failed: %v\n", err)
		}
		shutdownCancel()
		grpcServer.GracefulStop()
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			return serveErr
		}
		return nil
	}
}

func parseReplicatedRootPeers(values []string) (map[uint64]string, error) {
	peers := make(map[uint64]string, len(values))
	for _, raw := range values {
		parts := strings.SplitN(strings.TrimSpace(raw), "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer value %q (want nodeID=address)", raw)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer id in %q: %w", raw, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("invalid peer id in %q: must be > 0", raw)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return nil, fmt.Errorf("invalid peer value %q (empty address)", raw)
		}
		if _, ok := peers[id]; ok {
			return nil, fmt.Errorf("duplicate peer id %d", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

func splitCommaList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseCoordinatorGrantDuties(raw string) ([]rootproto.DutyID, error) {
	parts := splitCommaList(raw)
	out := make([]rootproto.DutyID, 0, len(parts))
	for _, part := range parts {
		duty := rootproto.DutyID(part)
		switch duty {
		case rootproto.DutyAllocID, rootproto.DutyTSO, rootproto.DutyRegionLookup:
			out = append(out, duty)
		default:
			return nil, fmt.Errorf("unknown coordinator grant duty %q", part)
		}
	}
	return out, nil
}

func dutyNames(duties []rootproto.DutyID) []string {
	out := make([]string, 0, len(duties))
	for _, duty := range duties {
		out = append(out, rootproto.DutyName(duty))
	}
	return out
}

func fsmetaBootstrapSplitBoundaries(bootstrap *config.FSMetaRegionBootstrap) ([][]byte, error) {
	if bootstrap == nil {
		return nil, nil
	}
	mounts := make([]model.MountIdentity, 0, len(bootstrap.Mounts))
	for _, mount := range bootstrap.Mounts {
		mounts = append(mounts, model.MountIdentity{
			MountID:    model.MountID(mount.MountID),
			MountKeyID: model.MountKeyID(mount.MountKeyID),
		})
	}
	boundaries, err := layout.BucketSplitBoundaries(mounts, bootstrap.BucketCount)
	if err != nil {
		return nil, fmt.Errorf("coordinator build fsmeta split boundaries: %w", err)
	}
	return boundaries, nil
}

func flagPassed(fs *flag.FlagSet, name string) bool {
	if fs == nil || name == "" {
		return false
	}
	passed := false
	fs.Visit(func(f *flag.Flag) {
		if f != nil && f.Name == name {
			passed = true
		}
	})
	return passed
}
