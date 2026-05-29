// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package sst

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/feichai0017/NoKV/engine/vfs"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
)

// ExportPayload materializes one SST snapshot and bundles it into
// a transport-safe payload.
func ExportPayload(src exportSource, workDir string, region localmeta.RegionMeta, fs vfs.FS) ([]byte, Meta, error) {
	var payload bytes.Buffer
	meta, err := ExportPayloadTo(&payload, src, workDir, region, fs)
	if err != nil {
		return nil, Meta{}, err
	}
	return payload.Bytes(), meta, nil
}

// ExportPayloadTo materializes one SST snapshot and writes its tar payload to w.
func ExportPayloadTo(w io.Writer, src exportSource, workDir string, region localmeta.RegionMeta, fs vfs.FS) (Meta, error) {
	if w == nil {
		return Meta{}, fmt.Errorf("snapshot: export payload requires writer")
	}
	dir, cleanup, err := prepareSnapshotTempDir(workDir, "sst-export-*", fs)
	if err != nil {
		return Meta{}, err
	}
	defer cleanup()
	snapshotDir := filepath.Join(dir, "snapshot")
	result, err := ExportDir(src, snapshotDir, region, fs)
	if err != nil {
		return Meta{}, err
	}
	if err := writePayload(w, snapshotDir, result.Meta, fs); err != nil {
		return Meta{}, err
	}
	return result.Meta, nil
}

// ImportPayload unpacks one SST snapshot payload into a temporary workdir and
// installs it through the external SST ingest path.
func ImportPayload(dst installSink, workDir string, payload []byte, fs vfs.FS) (*ImportResult, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("snapshot: empty sst payload")
	}
	return ImportPayloadFrom(dst, workDir, bytes.NewReader(payload), fs)
}

// ImportPayloadFrom unpacks one SST snapshot payload from r into a temporary
// workdir and installs it through the external SST ingest path.
func ImportPayloadFrom(dst installSink, workDir string, r io.Reader, fs vfs.FS) (*ImportResult, error) {
	if dst == nil {
		return nil, fmt.Errorf("snapshot: import sst payload requires sink")
	}
	if r == nil {
		return nil, fmt.Errorf("snapshot: import sst payload requires reader")
	}
	dir, cleanup, err := prepareSnapshotTempDir(workDir, "sst-import-*", fs)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	snapshotDir := filepath.Join(dir, "snapshot")
	if err := unpackPayload(r, snapshotDir, fs); err != nil {
		return nil, err
	}
	meta, err := ReadMeta(snapshotDir, fs)
	if err != nil {
		return nil, err
	}
	for _, table := range meta.Tables {
		if _, err := vfs.Ensure(fs).Stat(filepath.Join(snapshotDir, table.RelativePath)); err != nil {
			return nil, fmt.Errorf("snapshot: unpacked sst snapshot missing table %s: %w", table.RelativePath, err)
		}
	}
	return ImportDir(dst, snapshotDir, fs)
}

// ReadPayloadMeta decodes only the metadata embedded in one snapshot payload.
func ReadPayloadMeta(payload []byte) (Meta, error) {
	if len(payload) == 0 {
		return Meta{}, fmt.Errorf("snapshot: empty sst payload")
	}
	return ReadPayloadMetaFrom(bytes.NewReader(payload))
}

// ReadPayloadMetaFrom decodes only the metadata embedded in one snapshot payload.
func ReadPayloadMetaFrom(r io.Reader) (Meta, error) {
	if r == nil {
		return Meta{}, fmt.Errorf("snapshot: read sst payload requires reader")
	}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Meta{}, fmt.Errorf("snapshot: read sst payload tar: %w", err)
		}
		if filepath.Clean(hdr.Name) != sstSnapshotName {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return Meta{}, fmt.Errorf("snapshot: read sst payload meta: %w", err)
		}
		var meta Meta
		if err := json.Unmarshal(data, &meta); err != nil {
			return Meta{}, fmt.Errorf("snapshot: decode sst payload meta: %w", err)
		}
		if meta.Version != sstVersion {
			return Meta{}, fmt.Errorf("snapshot: unsupported sst version %d", meta.Version)
		}
		return meta, nil
	}
	return Meta{}, fmt.Errorf("snapshot: sst payload missing %s", sstSnapshotName)
}

func writePayload(w io.Writer, dir string, meta Meta, fs vfs.FS) error {
	fs = vfs.Ensure(fs)
	tw := tar.NewWriter(w)
	writeFile := func(rel string) error {
		path := filepath.Join(dir, rel)
		info, err := fs.Stat(path)
		if err != nil {
			return fmt.Errorf("snapshot: stat sst snapshot file %s: %w", path, err)
		}
		f, err := fs.OpenHandle(path)
		if err != nil {
			return fmt.Errorf("snapshot: open sst snapshot file %s: %w", path, err)
		}
		defer func() { _ = f.Close() }()
		hdr := &tar.Header{
			Name:    filepath.ToSlash(rel),
			Mode:    0o600,
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return fmt.Errorf("snapshot: write sst snapshot header %s: %w", rel, err)
		}
		if _, err := io.Copy(tw, f); err != nil {
			return fmt.Errorf("snapshot: write sst snapshot body %s: %w", rel, err)
		}
		return nil
	}
	if err := writeFile(sstSnapshotName); err != nil {
		return err
	}
	for _, table := range meta.Tables {
		if err := writeFile(table.RelativePath); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("snapshot: finalize sst snapshot payload: %w", err)
	}
	return nil
}

func unpackPayload(r io.Reader, dir string, fs vfs.FS) error {
	fs = vfs.Ensure(fs)
	if err := fs.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("snapshot: create sst snapshot dir %s: %w", dir, err)
	}
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("snapshot: read sst snapshot payload: %w", err)
		}
		name := path.Clean(filepath.ToSlash(hdr.Name))
		if name == "." || strings.HasPrefix(name, "../") || name == ".." || path.IsAbs(name) {
			return fmt.Errorf("snapshot: invalid sst snapshot path %q", hdr.Name)
		}
		targetPath, err := secureSnapshotPath(dir, name)
		if err != nil {
			return fmt.Errorf("snapshot: invalid sst snapshot path %q: %w", hdr.Name, err)
		}
		parent := filepath.Dir(targetPath)
		if err := fs.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("snapshot: create sst snapshot parent %s: %w", parent, err)
		}
		f, err := fs.OpenFileHandle(targetPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("snapshot: create sst snapshot file %s: %w", targetPath, err)
		}
		if _, err := io.Copy(f, tr); err != nil {
			_ = f.Close()
			return fmt.Errorf("snapshot: write sst snapshot file %s: %w", targetPath, err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("snapshot: sync sst snapshot file %s: %w", targetPath, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("snapshot: close sst snapshot file %s: %w", targetPath, err)
		}
	}
	_, err := ReadMeta(dir, fs)
	return err
}

func secureSnapshotPath(baseDir, rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("empty path")
	}
	baseDir = filepath.Clean(baseDir)
	targetPath := filepath.Join(baseDir, filepath.FromSlash(rel))
	relative, err := filepath.Rel(baseDir, targetPath)
	if err != nil {
		return "", err
	}
	relative = filepath.Clean(relative)
	if relative == "." || relative == "" {
		return "", fmt.Errorf("path must resolve to a file")
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes snapshot dir")
	}
	return targetPath, nil
}
