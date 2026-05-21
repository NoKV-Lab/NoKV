// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package artifact

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidArtifactPath     = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: invalid artifact path")
	ErrArtifactIsDirectory     = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: artifact is a directory")
	ErrArtifactIsFile          = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: artifact is a file")
	ErrInvalidArtifactMetadata = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: invalid artifact metadata")
	ErrInvalidBodyRef          = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: invalid body reference")
	ErrArtifactBodyNotFound    = nokverrors.New(nokverrors.KindNotFound, "sdk/artifact: artifact body not found")

	errNamespaceRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: namespace client is required")
	errBodyStoreRequired     = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: body store is required")
	errMountRequired         = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: mount is required")
	errBodyReaderRequired    = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: body reader is required")
	errBodyWriterRequired    = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: body writer is required")
	errBodyStoreRootRequired = nokverrors.New(nokverrors.KindInvalidArgument, "sdk/artifact: file body store root is required")
)
