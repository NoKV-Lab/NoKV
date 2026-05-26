// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package compile

import "github.com/feichai0017/NoKV/fsmeta/layout"

type ReadProgramKind uint8

const (
	ReadProgramLookup ReadProgramKind = iota + 1
	ReadProgramGetAttr
	ReadProgramReadSession
)

type ReadProgram struct {
	Kind      ReadProgramKind
	Plan      layout.OperationPlan
	Authority AuthorityPlan
	Footprint KeyFootprint
	Key       []byte
}

type DirectoryReadSource uint8

const (
	DirectoryReadSourceBase DirectoryReadSource = iota + 1
	DirectoryReadSourceOverlay
	DirectoryReadSourceOverlayOnly
)

type DirectoryReadStats struct {
	BaseRows        uint32
	OverlayRows     uint32
	OutputRows      uint32
	UsedDirIndex    bool
	UsedOverlayOnly bool
}

type DirectoryReadPlan struct {
	Plan           layout.OperationPlan
	Authority      AuthorityPlan
	Footprint      KeyFootprint
	Prefix         []byte
	StartKey       []byte
	Limit          uint32
	IncludeOverlay bool
	OverlayOnly    bool
	Source         DirectoryReadSource
}
