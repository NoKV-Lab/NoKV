// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package local

import (
	"context"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

// MountConfig defines the single local fsmeta mount.
type MountConfig struct {
	Mount     model.MountIdentity
	RootInode model.InodeID
}

// MountCatalog is the local mount-admission table. It owns one active mount and
// treats every subtree as one local authority.
type MountCatalog struct {
	admission fsmetaexec.MountAdmission
}

// NewMountCatalog constructs a single-mount local admission catalog.
func NewMountCatalog(cfg MountConfig) *MountCatalog {
	root := cfg.RootInode
	if root == 0 {
		root = model.RootInode
	}
	return &MountCatalog{admission: fsmetaexec.MountAdmission{
		MountID:       cfg.Mount.MountID,
		MountKeyID:    cfg.Mount.MountKeyID,
		RootInode:     root,
		SchemaVersion: 1,
	}}
}

// Admission returns the configured active mount record.
func (c *MountCatalog) Admission() fsmetaexec.MountAdmission {
	if c == nil {
		return fsmetaexec.MountAdmission{}
	}
	return c.admission
}

// ResolveMount implements fsmetaexec.MountResolver.
func (c *MountCatalog) ResolveMount(_ context.Context, mount model.MountID) (fsmetaexec.MountAdmission, error) {
	if c == nil || c.admission.MountID == "" || c.admission.MountID != mount {
		return fsmetaexec.MountAdmission{}, model.ErrMountNotRegistered
	}
	return c.admission, nil
}

// SameAuthority implements the local single-authority namespace model.
func (c *MountCatalog) SameAuthority(_ context.Context, mount model.MountID, _ model.InodeID, _ model.InodeID) (bool, error) {
	if c == nil || c.admission.MountID == "" || c.admission.MountID != mount {
		return false, model.ErrMountNotRegistered
	}
	return true, nil
}

// StartSubtreeHandoff is a no-op because local fsmeta has no rooted authority
// handoff layer.
func (c *MountCatalog) StartSubtreeHandoff(ctx context.Context, mount model.MountID, _ model.InodeID, _ uint64) error {
	_, err := c.ResolveMount(ctx, mount)
	return err
}

// CompleteSubtreeHandoff is a no-op because local fsmeta has no rooted
// authority handoff layer.
func (c *MountCatalog) CompleteSubtreeHandoff(ctx context.Context, mount model.MountID, _ model.InodeID, _ uint64) error {
	_, err := c.ResolveMount(ctx, mount)
	return err
}
