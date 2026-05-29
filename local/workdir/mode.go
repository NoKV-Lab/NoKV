// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workdir

import (
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/feichai0017/NoKV/storage/vfs"
)

// Mode describes the lifecycle mode of one workdir.
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModePreparing  Mode = "preparing"
	ModeSeeded     Mode = "seeded"
	ModeCluster    Mode = "cluster"
)

// FileName stores the explicit mode marker for one workdir.
const FileName = "MODE.json"

// State is the persisted mode marker for one workdir.
type State struct {
	Mode     Mode   `json:"mode"`
	StoreID  uint64 `json:"store_id,omitempty"`
	RegionID uint64 `json:"region_id,omitempty"`
	PeerID   uint64 `json:"peer_id,omitempty"`
}

// Read loads the persisted workdir mode. Missing files default to standalone.
func Read(workDir string) (State, error) {
	path := filepath.Join(workDir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if stderrors.Is(err, os.ErrNotExist) {
			return State{Mode: ModeStandalone}, nil
		}
		return State{}, fmt.Errorf("read mode file: %w", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, fmt.Errorf("decode mode file: %w", err)
	}
	switch state.Mode {
	case ModeStandalone, ModePreparing, ModeSeeded, ModeCluster:
		return state, nil
	default:
		return State{}, fmt.Errorf("unknown workdir mode %q", state.Mode)
	}
}

// ReadOnlyMode returns only the mode value for one workdir.
func ReadOnlyMode(workDir string) (Mode, error) {
	state, err := Read(workDir)
	if err != nil {
		return "", err
	}
	return state.Mode, nil
}

// Write persists the workdir mode marker using temp+sync+rename+syncdir.
func Write(workDir string, state State) error {
	workDir = filepath.Clean(workDir)
	if workDir == "" || workDir == "." {
		return fmt.Errorf("write mode file: workdir is required")
	}
	fs := vfs.Ensure(nil)
	path := filepath.Join(workDir, FileName)
	tmp := path + ".tmp"
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode mode file: %w", err)
	}
	f, err := fs.OpenFileHandle(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open mode temp file: %w", err)
	}
	writeErr := writeAll(f, payload)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("write mode temp file: %w", writeErr)
	}
	if syncErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("sync mode temp file: %w", syncErr)
	}
	if closeErr != nil {
		_ = fs.Remove(tmp)
		return fmt.Errorf("close mode temp file: %w", closeErr)
	}
	if err := fs.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename mode file: %w", err)
	}
	if err := vfs.SyncDir(fs, workDir); err != nil {
		return fmt.Errorf("sync mode parent dir: %w", err)
	}
	return nil
}

// Allowed reports whether actual is permitted by the provided allow-list.
// An empty allow-list means standalone-only.
func Allowed(allow []Mode, actual Mode) bool {
	if len(allow) == 0 {
		return actual == ModeStandalone
	}
	return slices.Contains(allow, actual)
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
