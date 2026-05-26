// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package fsmeta

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidKey       = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid key")
	ErrInvalidKeyKind   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid key kind")
	ErrInvalidValueKind = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta: invalid value kind")
)
