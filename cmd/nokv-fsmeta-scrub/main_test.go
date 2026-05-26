// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
)

type fakeScrubClient struct {
	dirs map[model.InodeID][]model.DentryAttrPair
	err  error
}

func (c *fakeScrubClient) ReadDirPlus(_ context.Context, req model.ReadDirRequest) ([]model.DentryAttrPair, error) {
	if c.err != nil {
		return nil, c.err
	}
	entries := c.dirs[req.Parent]
	start := 0
	for start < len(entries) && entries[start].Dentry.Name <= req.StartAfter {
		start++
	}
	limit := int(req.Limit)
	if limit == 0 || limit > len(entries)-start {
		limit = len(entries) - start
	}
	return append([]model.DentryAttrPair(nil), entries[start:start+limit]...), nil
}

func (c *fakeScrubClient) Close() error {
	return nil
}

func TestScrubMountReportsHealthyNamespace(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[model.InodeID][]model.DentryAttrPair{
		model.RootInode: {
			scrubPair(model.RootInode, "dir", 10, model.InodeTypeDirectory, 1),
			scrubPair(model.RootInode, "file", 11, model.InodeTypeFile, 2),
			scrubPair(model.RootInode, "hardlink", 11, model.InodeTypeFile, 2),
		},
		10: {
			scrubPair(10, "child", 12, model.InodeTypeFile, 1),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", model.RootInode, 2, 8)
	require.NoError(t, err)
	require.True(t, report.OK())
	require.Equal(t, uint64(2), report.Directories)
	require.Equal(t, uint64(4), report.Dentries)
	require.Equal(t, uint64(3), report.Inodes)
}

func TestScrubMountReportsInvariantIssues(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[model.InodeID][]model.DentryAttrPair{
		model.RootInode: {
			{
				Dentry: model.DentryRecord{Parent: model.RootInode, Name: "bad-type", Inode: 20, Type: model.InodeTypeFile},
				Inode:  model.InodeRecord{Inode: 20, Type: model.InodeTypeDirectory, LinkCount: 1},
			},
			scrubPair(model.RootInode, "link-a", 21, model.InodeTypeFile, 1),
			scrubPair(model.RootInode, "link-b", 21, model.InodeTypeFile, 1),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", model.RootInode, 8, 8)
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Contains(t, scrubIssueKinds(report.Issues), issueDentryTypeMismatch)
	require.Contains(t, scrubIssueKinds(report.Issues), issueLinkCountMismatch)
}

func TestScrubMountLimitsIssues(t *testing.T) {
	cli := &fakeScrubClient{dirs: map[model.InodeID][]model.DentryAttrPair{
		model.RootInode: {
			scrubPair(model.RootInode, "", 10, model.InodeTypeFile, 2),
			scrubPair(99, "wrong-parent", 11, model.InodeTypeFile, 2),
		},
	}}

	report, err := scrubMount(context.Background(), cli, "vol", model.RootInode, 8, 1)
	require.NoError(t, err)
	require.Len(t, report.Issues, 2)
	require.Equal(t, issueIssueLimitExhausted, report.Issues[1].Kind)
}

func TestScrubMountPropagatesReadDirError(t *testing.T) {
	want := errors.New("read failed")
	_, err := scrubMount(context.Background(), &fakeScrubClient{err: want}, "vol", model.RootInode, 8, 8)
	require.ErrorIs(t, err, want)
}

func scrubPair(parent model.InodeID, name string, inode model.InodeID, typ model.InodeType, links uint32) model.DentryAttrPair {
	return model.DentryAttrPair{
		Dentry: model.DentryRecord{Parent: parent, Name: name, Inode: inode, Type: typ},
		Inode:  model.InodeRecord{Inode: inode, Type: typ, LinkCount: links},
	}
}

func scrubIssueKinds(issues []scrubIssue) []scrubIssueKind {
	kinds := make([]scrubIssueKind, 0, len(issues))
	for _, issue := range issues {
		kinds = append(kinds, issue.Kind)
	}
	return kinds
}
