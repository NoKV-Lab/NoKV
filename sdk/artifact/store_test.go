// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
	"github.com/stretchr/testify/require"
)

func TestStorePutGetListStatDeleteWithLocalFSMetaRuntime(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	payload := []byte(`{"accuracy":0.99}`)
	source := writeTestFile(t, payload)

	info, err := store.PutFile(ctx, source, "runs/run-1/metrics.json")
	require.NoError(t, err)
	require.Equal(t, "runs/run-1/metrics.json", info.Path)
	require.False(t, info.IsDir)
	require.Equal(t, uint64(len(payload)), info.Size)
	require.Equal(t, "sha256:"+hexDigest(payload), info.Body.Digest)

	root, err := store.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, root, 1)
	require.Equal(t, "runs", root[0].Path)
	require.True(t, root[0].IsDir)

	runDir, err := store.List(ctx, "runs/run-1")
	require.NoError(t, err)
	require.Len(t, runDir, 1)
	require.Equal(t, "runs/run-1/metrics.json", runDir[0].Path)
	require.False(t, runDir[0].IsDir)
	require.Equal(t, uint64(len(payload)), runDir[0].Size)

	fileChildren, err := store.List(ctx, "runs/run-1/metrics.json")
	require.NoError(t, err)
	require.Empty(t, fileChildren)

	stat, err := store.Stat(ctx, "runs/run-1/metrics.json")
	require.NoError(t, err)
	require.Equal(t, info.Path, stat.Path)
	require.Equal(t, info.Body, stat.Body)

	download := filepath.Join(t.TempDir(), "downloaded.json")
	got, err := store.GetFile(ctx, "runs/run-1/metrics.json", download)
	require.NoError(t, err)
	require.Equal(t, info.Path, got.Path)
	downloaded, err := os.ReadFile(download)
	require.NoError(t, err)
	require.Equal(t, payload, downloaded)

	err = store.Delete(ctx, "runs/run-1/metrics.json")
	require.NoError(t, err)
	_, err = store.Stat(ctx, "runs/run-1/metrics.json")
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	_, err = store.GetFile(ctx, "runs/run-1/metrics.json", filepath.Join(t.TempDir(), "missing.json"))
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	remaining, err := store.List(ctx, "runs/run-1")
	require.NoError(t, err)
	require.Empty(t, remaining)
}

func TestStorePutOverwritesExistingArtifact(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	first := writeTestFile(t, []byte("first"))
	second := writeTestFile(t, []byte("second"))

	firstInfo, err := store.PutFile(ctx, first, "models/model.bin")
	require.NoError(t, err)
	secondInfo, err := store.PutFile(ctx, second, "models/model.bin")
	require.NoError(t, err)
	require.Equal(t, "sha256:"+hexDigest([]byte("first")), firstInfo.Body.Digest)
	require.Equal(t, "sha256:"+hexDigest([]byte("second")), secondInfo.Body.Digest)

	download := filepath.Join(t.TempDir(), "model.bin")
	_, err = store.GetFile(ctx, "models/model.bin", download)
	require.NoError(t, err)
	got, err := os.ReadFile(download)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), got)
}

func TestStoreRejectsUnsafeArtifactPaths(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	source := writeTestFile(t, []byte("payload"))

	for _, path := range []string{"", ".", "/absolute", "a//b", "a/../b", "../b", "a\\b", "a\x00b"} {
		_, err := store.PutFile(ctx, source, path)
		require.ErrorIs(t, err, ErrInvalidArtifactPath, path)
	}
}

func TestStoreDeleteRecursivelyRemovesDirectory(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	first := writeTestFile(t, []byte("first"))
	second := writeTestFile(t, []byte("second"))

	_, err := store.PutFile(ctx, first, "dir/file.txt")
	require.NoError(t, err)
	_, err = store.PutFile(ctx, second, "dir/nested/child.txt")
	require.NoError(t, err)

	err = store.Delete(ctx, "dir")
	require.NoError(t, err)
	_, err = store.Stat(ctx, "dir")
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	root, err := store.List(ctx, "")
	require.NoError(t, err)
	require.Empty(t, root)
}

func TestStoreDeleteRootRemovesChildrenButKeepsRootUsable(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	first := writeTestFile(t, []byte("first"))
	second := writeTestFile(t, []byte("second"))

	_, err := store.PutFile(ctx, first, "a.txt")
	require.NoError(t, err)
	_, err = store.PutFile(ctx, second, "dir/b.txt")
	require.NoError(t, err)

	err = store.Delete(ctx, "")
	require.NoError(t, err)
	root, err := store.List(ctx, "")
	require.NoError(t, err)
	require.Empty(t, root)

	next := writeTestFile(t, []byte("next"))
	_, err = store.PutFile(ctx, next, "next.txt")
	require.NoError(t, err)
	root, err = store.List(ctx, "")
	require.NoError(t, err)
	require.Len(t, root, 1)
	require.Equal(t, "next.txt", root[0].Path)
}

func TestStoreDeleteDoesNotEagerlyDeleteSharedContentAddressedBody(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	source := writeTestFile(t, []byte("shared"))

	first, err := store.PutFile(ctx, source, "first.txt")
	require.NoError(t, err)
	second, err := store.PutFile(ctx, source, "second.txt")
	require.NoError(t, err)
	require.Equal(t, first.Body, second.Body)

	err = store.Delete(ctx, "first.txt")
	require.NoError(t, err)

	download := filepath.Join(t.TempDir(), "second.txt")
	_, err = store.GetFile(ctx, "second.txt", download)
	require.NoError(t, err)
	got, err := os.ReadFile(download)
	require.NoError(t, err)
	require.Equal(t, []byte("shared"), got)
}

func TestStoreGetRejectsDirectory(t *testing.T) {
	ctx := context.Background()
	store := openLocalTestStore(t)
	source := writeTestFile(t, []byte("payload"))

	_, err := store.PutFile(ctx, source, "dir/file.txt")
	require.NoError(t, err)

	var out bytes.Buffer
	_, err = store.Get(ctx, "dir", &out)
	require.ErrorIs(t, err, ErrArtifactIsDirectory)
}

func TestStoreAcceptsNilContext(t *testing.T) {
	store := openLocalTestStore(t)
	source := writeTestFile(t, []byte("payload"))

	_, err := store.PutFile(nil, source, "nil-context/file.txt")
	require.NoError(t, err)
	entries, err := store.List(nil, "nil-context")
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func openLocalTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	rt, err := fsmetalocal.Open(ctx, fsmetalocal.Options{
		WorkDir: t.TempDir(),
		Mount:   fsmeta.MountIdentity{MountID: "vol", MountKeyID: 1},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rt.Close())
	})
	bodies, err := NewFileBodyStore(filepath.Join(t.TempDir(), "bodies"))
	require.NoError(t, err)
	store, err := NewStore(Options{
		Namespace: rt.Executor,
		BodyStore: bodies,
		Mount:     "vol",
	})
	require.NoError(t, err)
	return store
}

func writeTestFile(t *testing.T, payload []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "artifact")
	require.NoError(t, os.WriteFile(path, payload, 0o644))
	return path
}

func hexDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func TestStoreConstructorRequiresDependencies(t *testing.T) {
	_, err := NewStore(Options{})
	require.ErrorIs(t, err, errNamespaceRequired)

	_, err = NewStore(Options{Namespace: fakeNamespace{}})
	require.ErrorIs(t, err, errBodyStoreRequired)
}

type fakeNamespace struct{}

func (fakeNamespace) Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	return fsmeta.CreateResult{}, errors.New("not implemented")
}

func (fakeNamespace) LookupPlus(context.Context, fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error) {
	return fsmeta.DentryAttrPair{}, errors.New("not implemented")
}

func (fakeNamespace) ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	return nil, errors.New("not implemented")
}

func (fakeNamespace) Rename(context.Context, fsmeta.RenameRequest) error {
	return errors.New("not implemented")
}

func (fakeNamespace) RenameReplace(context.Context, fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error) {
	return fsmeta.RenameReplaceResult{}, errors.New("not implemented")
}

func (fakeNamespace) Remove(context.Context, fsmeta.RemoveRequest) (fsmeta.RemoveResult, error) {
	return fsmeta.RemoveResult{}, errors.New("not implemented")
}

func (fakeNamespace) RemoveDirectory(context.Context, fsmeta.RemoveDirectoryRequest) error {
	return errors.New("not implemented")
}
