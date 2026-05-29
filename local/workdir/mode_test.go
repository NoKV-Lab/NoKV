// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workdir

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMissingModeDefaultsToStandalone(t *testing.T) {
	state, err := Read(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if state != (State{Mode: ModeStandalone}) {
		t.Fatalf("missing mode state=%#v, want standalone", state)
	}
}

func TestWriteReadModeRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := State{
		Mode:     ModeCluster,
		StoreID:  2,
		RegionID: 91,
		PeerID:   201,
	}
	if err := Write(dir, in); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, FileName+".tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary mode file should not remain, stat err=%v", err)
	}

	out, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("mode round trip got %#v want %#v", out, in)
	}

	only, err := ReadOnlyMode(dir)
	if err != nil {
		t.Fatal(err)
	}
	if only != ModeCluster {
		t.Fatalf("ReadOnlyMode=%q, want %q", only, ModeCluster)
	}
}

func TestReadRejectsUnknownMode(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(`{"mode":"unknown-cluster"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := Read(dir)
	if err == nil || !strings.Contains(err.Error(), "unknown workdir mode") {
		t.Fatalf("expected unknown mode error, got %v", err)
	}
}

func TestAllowedDefaultsToStandaloneOnly(t *testing.T) {
	if !Allowed(nil, ModeStandalone) {
		t.Fatal("empty allow-list should allow standalone")
	}
	if Allowed(nil, ModeCluster) {
		t.Fatal("empty allow-list should reject cluster")
	}
	if !Allowed([]Mode{ModePreparing, ModeSeeded}, ModeSeeded) {
		t.Fatal("explicit allow-list should allow seeded")
	}
	if Allowed([]Mode{ModePreparing, ModeSeeded}, ModeCluster) {
		t.Fatal("explicit allow-list should reject missing mode")
	}
}
