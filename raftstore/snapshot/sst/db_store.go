// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst

import (
	"fmt"
	"io"

	local "github.com/feichai0017/NoKV/local"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// DBStore adapts the embedded DB external-SST hooks into raftstore snapshots.
type DBStore struct {
	db *local.DB
}

func NewDBStore(db *local.DB) DBStore {
	return DBStore{db: db}
}

func (s DBStore) requireDB() (*local.DB, error) {
	if s.db == nil {
		return nil, fmt.Errorf("raftstore/snapshot: db requires open db")
	}
	if s.db.ExternalSSTOptions() == nil {
		return nil, fmt.Errorf("raftstore/snapshot: db requires open db")
	}
	return s.db, nil
}

func (s DBStore) workDir() (string, error) {
	db, err := s.requireDB()
	if err != nil {
		return "", err
	}
	workDir := db.WorkDir()
	if workDir == "" {
		return "", fmt.Errorf("raftstore/snapshot: db workdir is required")
	}
	return workDir, nil
}

func (s DBStore) ExportSnapshotDir(dir string, region localmeta.RegionMeta) (*ExportResult, error) {
	db, err := s.requireDB()
	if err != nil {
		return nil, err
	}
	return ExportDir(db, dir, region, nil)
}

func (s DBStore) ImportSnapshotDir(dir string) (*ImportResult, error) {
	db, err := s.requireDB()
	if err != nil {
		return nil, err
	}
	return ImportDir(db, dir, nil)
}

func (s DBStore) ExportSnapshot(region localmeta.RegionMeta) ([]byte, error) {
	workDir, err := s.workDir()
	if err != nil {
		return nil, err
	}
	payload, _, err := ExportPayload(s.db, workDir, region, nil)
	return payload, err
}

func (s DBStore) ExportSnapshotTo(w io.Writer, region localmeta.RegionMeta) (Meta, error) {
	workDir, err := s.workDir()
	if err != nil {
		return Meta{}, err
	}
	return ExportPayloadTo(w, s.db, workDir, region, nil)
}

func (s DBStore) ImportSnapshot(payload []byte) (*ImportResult, error) {
	workDir, err := s.workDir()
	if err != nil {
		return nil, err
	}
	return ImportPayload(s.db, workDir, payload, nil)
}

func (s DBStore) ImportSnapshotFrom(r io.Reader) (*ImportResult, error) {
	workDir, err := s.workDir()
	if err != nil {
		return nil, err
	}
	return ImportPayloadFrom(s.db, workDir, r, nil)
}

var _ SnapshotStore = DBStore{}
