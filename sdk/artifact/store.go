// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
)

// Store coordinates fsmeta namespace metadata with an external artifact body
// store.
type Store struct {
	namespace       NamespaceClient
	bodies          BodyStore
	mount           fsmeta.MountID
	root            fsmeta.InodeID
	fileMode        uint32
	directoryMode   uint32
	stageNamePrefix string
	clock           func() time.Time
	stageCounter    atomic.Uint64
}

// NewStore constructs an artifact namespace store.
func NewStore(opts Options) (*Store, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &Store{
		namespace:       opts.Namespace,
		bodies:          opts.BodyStore,
		mount:           opts.Mount,
		root:            opts.rootInode(),
		fileMode:        opts.fileMode(),
		directoryMode:   opts.directoryMode(),
		stageNamePrefix: opts.stageNamePrefix(),
		clock:           opts.clock(),
	}, nil
}

// Put stores body under artifactPath and publishes the fsmeta namespace entry.
// Existing file artifacts are replaced atomically by the fsmeta namespace
// commit, so readers observe either the old body reference or the new one.
func (s *Store) Put(ctx context.Context, artifactPath string, body io.Reader) (ArtifactInfo, error) {
	if body == nil {
		return ArtifactInfo{}, errBodyReaderRequired
	}
	ctx = normalizeContext(ctx)
	parts, err := splitArtifactPath(artifactPath, false)
	if err != nil {
		return ArtifactInfo{}, err
	}
	parent, err := s.ensureParentDirectories(ctx, parts[:len(parts)-1])
	if err != nil {
		return ArtifactInfo{}, err
	}
	finalName := parts[len(parts)-1]

	ref, err := s.bodies.Put(ctx, body)
	if err != nil {
		return ArtifactInfo{}, err
	}
	opaque, err := encodeArtifactOpaqueAttrs(ref)
	if err != nil {
		return ArtifactInfo{}, err
	}
	now := s.clock().UnixNano()
	stageName := s.nextStageName(finalName)
	if _, err := s.namespace.Create(ctx, fsmeta.CreateRequest{
		Mount:  s.mount,
		Parent: parent,
		Name:   stageName,
		Attrs: fsmeta.CreateAttrs{
			Type:          fsmeta.InodeTypeFile,
			Size:          ref.Size,
			Mode:          s.fileMode,
			CreatedUnixNs: now,
			UpdatedUnixNs: now,
			OpaqueAttrs:   opaque,
		},
	}); err != nil {
		return ArtifactInfo{}, err
	}
	if _, err := s.namespace.RenameReplace(ctx, fsmeta.RenameReplaceRequest{
		Mount:      s.mount,
		FromParent: parent,
		FromName:   stageName,
		ToParent:   parent,
		ToName:     finalName,
	}); err != nil {
		return ArtifactInfo{}, err
	}
	return ArtifactInfo{
		Path:  normalizeArtifactPath(parts),
		IsDir: false,
		Size:  ref.Size,
		Body:  ref,
	}, nil
}

// PutFile stores the local file at artifactPath.
func (s *Store) PutFile(ctx context.Context, localPath, artifactPath string) (ArtifactInfo, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return ArtifactInfo{}, err
	}
	defer file.Close()
	return s.Put(ctx, artifactPath, file)
}

// Get copies the artifact body into w and returns its metadata.
func (s *Store) Get(ctx context.Context, artifactPath string, w io.Writer) (ArtifactInfo, error) {
	if w == nil {
		return ArtifactInfo{}, errBodyWriterRequired
	}
	ctx = normalizeContext(ctx)
	info, err := s.Stat(ctx, artifactPath)
	if err != nil {
		return ArtifactInfo{}, err
	}
	if info.IsDir {
		return ArtifactInfo{}, ErrArtifactIsDirectory
	}
	if err := s.bodies.Get(ctx, info.Body, w); err != nil {
		return ArtifactInfo{}, err
	}
	return info, nil
}

// GetFile downloads the artifact body into localPath.
func (s *Store) GetFile(ctx context.Context, artifactPath, localPath string) (ArtifactInfo, error) {
	ctx = normalizeContext(ctx)
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ArtifactInfo{}, err
	}
	tmp, err := os.CreateTemp(dir, ".nokv-artifact-*")
	if err != nil {
		return ArtifactInfo{}, err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	info, getErr := s.Get(ctx, artifactPath, tmp)
	if closeErr := tmp.Close(); closeErr != nil && getErr == nil {
		getErr = closeErr
	}
	if getErr != nil {
		return ArtifactInfo{}, getErr
	}
	if err := os.Rename(tmpName, localPath); err != nil {
		return ArtifactInfo{}, err
	}
	committed = true
	return info, nil
}

// Stat returns exact artifact metadata.
func (s *Store) Stat(ctx context.Context, artifactPath string) (ArtifactInfo, error) {
	ctx = normalizeContext(ctx)
	parts, err := splitArtifactPath(artifactPath, false)
	if err != nil {
		return ArtifactInfo{}, err
	}
	pair, err := s.resolvePath(ctx, parts)
	if err != nil {
		return ArtifactInfo{}, err
	}
	return infoFromPair(normalizeArtifactPath(parts), pair)
}

// List returns direct children. Listing a file returns an empty slice to match
// MLflow ArtifactRepository semantics.
func (s *Store) List(ctx context.Context, artifactPath string) ([]ArtifactInfo, error) {
	ctx = normalizeContext(ctx)
	parts, err := splitArtifactPath(artifactPath, true)
	if err != nil {
		return nil, err
	}
	parent := s.root
	prefix := ""
	if len(parts) > 0 {
		pair, err := s.resolvePath(ctx, parts)
		if err != nil {
			return nil, err
		}
		if pair.Inode.Type != fsmeta.InodeTypeDirectory {
			return []ArtifactInfo{}, nil
		}
		parent = pair.Inode.Inode
		prefix = normalizeArtifactPath(parts)
	}
	pairs, err := fsmetaclient.ReadDirPlusAll(ctx, s.namespace, fsmeta.ReadDirRequest{
		Mount:  s.mount,
		Parent: parent,
		Limit:  fsmeta.DefaultReadDirLimit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ArtifactInfo, 0, len(pairs))
	for _, pair := range pairs {
		childPath := pair.Dentry.Name
		if prefix != "" {
			childPath = path.Join(prefix, pair.Dentry.Name)
		}
		info, err := infoFromPair(childPath, pair)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// Delete removes an artifact path from fsmeta. Directories are removed
// recursively, bottom-up. The root path deletes its children but leaves the root
// inode intact.
func (s *Store) Delete(ctx context.Context, artifactPath string) error {
	ctx = normalizeContext(ctx)
	parts, err := splitArtifactPath(artifactPath, true)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return s.deleteDirectoryChildren(ctx, s.root)
	}
	pair, err := s.resolvePath(ctx, parts)
	if err != nil {
		return err
	}
	if pair.Inode.Type == fsmeta.InodeTypeDirectory {
		if err := s.deleteDirectoryChildren(ctx, pair.Inode.Inode); err != nil {
			return err
		}
		return s.namespace.RemoveDirectory(ctx, fsmeta.RemoveDirectoryRequest{
			Mount:  s.mount,
			Parent: pair.Dentry.Parent,
			Name:   pair.Dentry.Name,
		})
	}
	return s.deleteFileDentry(ctx, pair)
}

func (s *Store) deleteDirectoryChildren(ctx context.Context, parent fsmeta.InodeID) error {
	pairs, err := fsmetaclient.ReadDirPlusAll(ctx, s.namespace, fsmeta.ReadDirRequest{
		Mount:  s.mount,
		Parent: parent,
		Limit:  fsmeta.DefaultReadDirLimit,
	})
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		if pair.Inode.Type == fsmeta.InodeTypeDirectory {
			if err := s.deleteDirectoryChildren(ctx, pair.Inode.Inode); err != nil {
				return err
			}
			if err := s.namespace.RemoveDirectory(ctx, fsmeta.RemoveDirectoryRequest{
				Mount:  s.mount,
				Parent: pair.Dentry.Parent,
				Name:   pair.Dentry.Name,
			}); err != nil {
				return err
			}
			continue
		}
		if err := s.deleteFileDentry(ctx, pair); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) deleteFileDentry(ctx context.Context, pair fsmeta.DentryAttrPair) error {
	_, err := s.namespace.Remove(ctx, fsmeta.RemoveRequest{
		Mount:  s.mount,
		Parent: pair.Dentry.Parent,
		Name:   pair.Dentry.Name,
	})
	return err
}

func (s *Store) ensureParentDirectories(ctx context.Context, parts []string) (fsmeta.InodeID, error) {
	parent := s.root
	for _, name := range parts {
		pair, err := s.namespace.LookupPlus(ctx, fsmeta.LookupRequest{Mount: s.mount, Parent: parent, Name: name})
		if err == nil {
			if pair.Inode.Type != fsmeta.InodeTypeDirectory {
				return 0, fmt.Errorf("%w: %s", ErrArtifactIsFile, name)
			}
			parent = pair.Inode.Inode
			continue
		}
		if !errors.Is(err, fsmeta.ErrNotFound) {
			return 0, err
		}
		now := s.clock().UnixNano()
		created, err := s.namespace.Create(ctx, fsmeta.CreateRequest{
			Mount:  s.mount,
			Parent: parent,
			Name:   name,
			Attrs: fsmeta.CreateAttrs{
				Type:          fsmeta.InodeTypeDirectory,
				Mode:          s.directoryMode,
				CreatedUnixNs: now,
				UpdatedUnixNs: now,
			},
		})
		if err == nil {
			parent = created.Inode.Inode
			continue
		}
		if !errors.Is(err, fsmeta.ErrExists) {
			return 0, err
		}
		pair, err = s.namespace.LookupPlus(ctx, fsmeta.LookupRequest{Mount: s.mount, Parent: parent, Name: name})
		if err != nil {
			return 0, err
		}
		if pair.Inode.Type != fsmeta.InodeTypeDirectory {
			return 0, fmt.Errorf("%w: %s", ErrArtifactIsFile, name)
		}
		parent = pair.Inode.Inode
	}
	return parent, nil
}

func (s *Store) resolvePath(ctx context.Context, parts []string) (fsmeta.DentryAttrPair, error) {
	parent := s.root
	var pair fsmeta.DentryAttrPair
	for i, name := range parts {
		got, err := s.namespace.LookupPlus(ctx, fsmeta.LookupRequest{Mount: s.mount, Parent: parent, Name: name})
		if err != nil {
			return fsmeta.DentryAttrPair{}, err
		}
		if i < len(parts)-1 && got.Inode.Type != fsmeta.InodeTypeDirectory {
			return fsmeta.DentryAttrPair{}, fmt.Errorf("%w: %s", ErrArtifactIsFile, name)
		}
		pair = got
		parent = got.Inode.Inode
	}
	return pair, nil
}

func infoFromPair(artifactPath string, pair fsmeta.DentryAttrPair) (ArtifactInfo, error) {
	if pair.Inode.Type == fsmeta.InodeTypeDirectory {
		return ArtifactInfo{Path: artifactPath, IsDir: true}, nil
	}
	ref, err := decodeArtifactOpaqueAttrs(pair.Inode.OpaqueAttrs)
	if err != nil {
		return ArtifactInfo{}, err
	}
	return ArtifactInfo{
		Path:  artifactPath,
		IsDir: false,
		Size:  pair.Inode.Size,
		Body:  ref,
	}, nil
}

func (s *Store) nextStageName(finalName string) string {
	n := s.stageCounter.Add(1)
	return fmt.Sprintf("%s%x-%s", s.stageNamePrefix, n, finalName)
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
