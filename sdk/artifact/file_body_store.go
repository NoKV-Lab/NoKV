// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// FileBodyStore stores artifact bodies as content-addressed files under one
// local directory.
type FileBodyStore struct {
	root string
}

// NewFileBodyStore opens a local filesystem body store rooted at dir.
func NewFileBodyStore(dir string) (*FileBodyStore, error) {
	if dir == "" {
		return nil, errBodyStoreRootRequired
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileBodyStore{root: dir}, nil
}

// Put stores body bytes and returns a content-addressed reference.
func (s *FileBodyStore) Put(ctx context.Context, body io.Reader) (BodyRef, error) {
	if s == nil || s.root == "" {
		return BodyRef{}, errBodyStoreRootRequired
	}
	if body == nil {
		return BodyRef{}, errBodyReaderRequired
	}
	ctx = normalizeContext(ctx)
	tmpDir := filepath.Join(s.root, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return BodyRef{}, err
	}
	tmp, err := os.CreateTemp(tmpDir, "put-*")
	if err != nil {
		return BodyRef{}, err
	}
	tmpName := tmp.Name()
	keepTemp := false
	defer func() {
		if !keepTemp {
			_ = os.Remove(tmpName)
		}
	}()

	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hash), contextReader{ctx: ctx, r: body})
	if copyErr != nil {
		_ = tmp.Close()
		return BodyRef{}, copyErr
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return BodyRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return BodyRef{}, err
	}

	digest := hex.EncodeToString(hash.Sum(nil))
	key := path.Join("objects", digest[:2], digest)
	target := filepath.Join(s.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return BodyRef{}, err
	}
	if _, err := os.Stat(target); err == nil {
		return BodyRef{Store: "file", Key: key, Digest: "sha256:" + digest, Size: uint64(n)}, nil
	} else if !os.IsNotExist(err) {
		return BodyRef{}, err
	}
	if err := os.Rename(tmpName, target); err != nil {
		return BodyRef{}, err
	}
	keepTemp = true
	return BodyRef{Store: "file", Key: key, Digest: "sha256:" + digest, Size: uint64(n)}, nil
}

// Get copies the referenced body into w.
func (s *FileBodyStore) Get(ctx context.Context, ref BodyRef, w io.Writer) error {
	if s == nil || s.root == "" {
		return errBodyStoreRootRequired
	}
	if w == nil {
		return errBodyWriterRequired
	}
	filePath, err := s.pathForRef(ref)
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return ErrArtifactBodyNotFound
	}
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(w, contextReader{ctx: normalizeContext(ctx), r: file})
	return err
}

// Delete removes the referenced body. Missing content is treated as already
// deleted because fsmeta is the authority for namespace visibility.
func (s *FileBodyStore) Delete(_ context.Context, ref BodyRef) error {
	if s == nil || s.root == "" {
		return errBodyStoreRootRequired
	}
	filePath, err := s.pathForRef(ref)
	if err != nil {
		return err
	}
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *FileBodyStore) pathForRef(ref BodyRef) (string, error) {
	if err := validateBodyRef(ref); err != nil {
		return "", err
	}
	if ref.Store != "file" {
		return "", fmt.Errorf("%w: store %q", ErrInvalidBodyRef, ref.Store)
	}
	clean := filepath.Clean(filepath.FromSlash(ref.Key))
	if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: key %q", ErrInvalidBodyRef, ref.Key)
	}
	return filepath.Join(s.root, clean), nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
