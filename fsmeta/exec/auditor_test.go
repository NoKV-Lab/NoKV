// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestAuditMountReportsHealthyNamespace(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 1})
	seedDentry(t, runner, "vol", fsmeta.RootInode, "file", 22)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{BatchSize: 1})
	require.NoError(t, err)
	require.True(t, report.OK())
	require.Equal(t, fsmeta.MountID("vol"), report.Mount)
	require.Equal(t, uint64(10), report.ReadVersion)
	require.Equal(t, uint64(2), report.Inodes)
	require.Equal(t, uint64(1), report.Dentries)
}

func TestAuditMountReportsDentryMissingInode(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedDentry(t, runner, "vol", fsmeta.RootInode, "missing", 99)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{})
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Len(t, report.Issues, 1)
	require.Equal(t, AuditDentryMissingInode, report.Issues[0].Kind)
	require.Equal(t, fsmeta.InodeID(99), report.Issues[0].Inode)
	require.Equal(t, "missing", report.Issues[0].Name)
}

func TestAuditMountReportsMissingRootInode(t *testing.T) {
	runner := newFakeRunner()
	rootKey, err := fsmeta.EncodeInodeKey(testMountIdentity, fsmeta.RootInode)
	require.NoError(t, err)
	delete(runner.data, string(rootKey))
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{})
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Len(t, report.Issues, 1)
	require.Equal(t, AuditRootMissing, report.Issues[0].Kind)
	require.Equal(t, fsmeta.RootInode, report.Issues[0].Inode)
}

func TestAuditMountReportsLinkCountMismatch(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeFile, LinkCount: 2})
	seedDentry(t, runner, "vol", fsmeta.RootInode, "file", 22)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{})
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Len(t, report.Issues, 1)
	require.Equal(t, AuditLinkCountMismatch, report.Issues[0].Kind)
	require.Equal(t, fsmeta.InodeID(22), report.Issues[0].Inode)
}

func TestAuditMountReportsDentryTypeMismatch(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: 22, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedDentry(t, runner, "vol", fsmeta.RootInode, "file", 22)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{})
	require.NoError(t, err)
	require.False(t, report.OK())
	require.Len(t, report.Issues, 1)
	require.Equal(t, AuditDentryTypeMismatch, report.Issues[0].Kind)
}

func TestAuditMountLimitsIssues(t *testing.T) {
	runner := newFakeRunner()
	seedInode(t, runner, "vol", fsmeta.InodeRecord{Inode: fsmeta.RootInode, Type: fsmeta.InodeTypeDirectory, LinkCount: 1})
	seedDentry(t, runner, "vol", fsmeta.RootInode, "a", 10)
	seedDentry(t, runner, "vol", fsmeta.RootInode, "b", 11)
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	report, err := executor.AuditMount(context.Background(), "vol", 10, AuditOptions{MaxIssues: 1})
	require.NoError(t, err)
	require.Len(t, report.Issues, 2)
	require.Equal(t, AuditDentryMissingInode, report.Issues[0].Kind)
	require.Equal(t, AuditIssueLimitExhausted, report.Issues[1].Kind)
}
