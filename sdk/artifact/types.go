// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import (
	"context"
	"io"

	"github.com/feichai0017/NoKV/fsmeta"
)

// NamespaceClient is the narrow fsmeta surface required by the artifact SDK.
// fsmeta/exec.Executor and the typed fsmeta gRPC client both satisfy this
// shape.
type NamespaceClient interface {
	Create(context.Context, fsmeta.CreateRequest) (fsmeta.CreateResult, error)
	LookupPlus(context.Context, fsmeta.LookupRequest) (fsmeta.DentryAttrPair, error)
	ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	Rename(context.Context, fsmeta.RenameRequest) error
	RenameReplace(context.Context, fsmeta.RenameReplaceRequest) (fsmeta.RenameReplaceResult, error)
	Unlink(context.Context, fsmeta.UnlinkRequest) error
}

// BodyStore owns artifact bytes outside fsmeta. Implementations should return a
// compact reference that can fit in fsmeta.InodeRecord.OpaqueAttrs.
type BodyStore interface {
	Put(context.Context, io.Reader) (BodyRef, error)
	Get(context.Context, BodyRef, io.Writer) error
	Delete(context.Context, BodyRef) error
}

// BodyRef is the durable reference stored in artifact inode metadata.
type BodyRef struct {
	Store     string `json:"store"`
	Key       string `json:"key"`
	Digest    string `json:"digest,omitempty"`
	Size      uint64 `json:"size"`
	MediaType string `json:"media_type,omitempty"`
	URI       string `json:"uri,omitempty"`
}

// ArtifactInfo is the SDK view returned by stat, list, put, and get calls.
type ArtifactInfo struct {
	Path  string
	IsDir bool
	Size  uint64
	Body  BodyRef
}
