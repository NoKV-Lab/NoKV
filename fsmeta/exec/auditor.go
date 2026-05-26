// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/feichai0017/NoKV/fsmeta/layout"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

const defaultAuditBatchSize uint32 = 256

// AuditIssueKind classifies one fsmeta namespace invariant violation.
type AuditIssueKind string

const (
	AuditInvalidKey          AuditIssueKind = "invalid_key"
	AuditInvalidValue        AuditIssueKind = "invalid_value"
	AuditInodeKeyMismatch    AuditIssueKind = "inode_key_mismatch"
	AuditDentryKeyMismatch   AuditIssueKind = "dentry_key_mismatch"
	AuditDentryMissingInode  AuditIssueKind = "dentry_missing_inode"
	AuditDentryTypeMismatch  AuditIssueKind = "dentry_type_mismatch"
	AuditInodeUnreferenced   AuditIssueKind = "inode_unreferenced"
	AuditLinkCountMismatch   AuditIssueKind = "link_count_mismatch"
	AuditRootMissing         AuditIssueKind = "root_missing"
	AuditIssueLimitExhausted AuditIssueKind = "issue_limit_exhausted"
)

// AuditIssue is one detected metadata inconsistency. It is diagnostic state;
// the auditor never repairs data behind the caller's back.
type AuditIssue struct {
	Kind   AuditIssueKind
	Key    []byte
	Inode  model.InodeID
	Parent model.InodeID
	Name   string
	Detail string
}

// AuditOptions bounds one online fsmeta scrub pass.
type AuditOptions struct {
	// RootInode is skipped when checking "inode must have a dentry reference".
	// Zero uses model.RootInode.
	RootInode model.InodeID
	// BatchSize is the number of KVs requested per scan. Zero uses the default.
	BatchSize uint32
	// MaxIssues stops recording after this many issues. Zero means unlimited.
	MaxIssues int
}

// AuditReport summarizes one read-only namespace scrub pass.
type AuditReport struct {
	Mount       model.MountID
	ReadVersion uint64
	Inodes      uint64
	Dentries    uint64
	Issues      []AuditIssue
}

func (r AuditReport) OK() bool {
	return len(r.Issues) == 0
}

// AuditMount scans one mount at a stable read version and checks namespace
// invariants that must hold after every committed fsmeta mutation.
func (e *Executor) AuditMount(ctx context.Context, mount model.MountID, readVersion uint64, opt AuditOptions) (AuditReport, error) {
	if e == nil || e.runner == nil {
		return AuditReport{}, errAuditorRunnerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if readVersion == 0 {
		var err error
		readVersion, err = e.reserveReadVersion(ctx)
		if err != nil {
			return AuditReport{}, err
		}
	}
	rootInode := opt.RootInode
	if rootInode == 0 {
		rootInode = model.RootInode
	}
	batchSize := opt.BatchSize
	if batchSize == 0 {
		batchSize = defaultAuditBatchSize
	}
	record, err := e.resolveActiveMount(ctx, mount)
	if err != nil {
		return AuditReport{}, err
	}
	identity := record.Identity()
	start, end, err := layout.EncodeMountKeyRange(identity)
	if err != nil {
		return AuditReport{}, err
	}

	report := AuditReport{Mount: mount, ReadVersion: readVersion}
	inodes := make(map[model.InodeID]model.InodeRecord)
	dentries := make([]model.DentryRecord, 0)
	refs := make(map[model.InodeID]uint32)
	addIssue := func(issue AuditIssue) bool {
		if opt.MaxIssues > 0 && len(report.Issues) >= opt.MaxIssues {
			report.Issues = append(report.Issues, AuditIssue{Kind: AuditIssueLimitExhausted, Detail: "issue limit reached"})
			return false
		}
		issue.Key = append([]byte(nil), issue.Key...)
		report.Issues = append(report.Issues, issue)
		return true
	}

	next := start
	for {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		rows, err := e.runner.Scan(ctx, next, batchSize, readVersion)
		if err != nil {
			return report, err
		}
		if len(rows) == 0 {
			break
		}
		advanced := false
		for _, row := range rows {
			if len(end) > 0 && bytes.Compare(row.Key, end) >= 0 {
				finishAuditReport(inodes, dentries, refs, rootInode, addIssue)
				return report, nil
			}
			kind, err := layout.KeyKindOf(row.Key)
			if err != nil {
				if !addIssue(AuditIssue{Kind: AuditInvalidKey, Key: row.Key, Detail: err.Error()}) {
					return report, nil
				}
				continue
			}
			switch kind {
			case layout.KeyKindInode:
				record, err := layout.DecodeInodeValue(row.Value)
				if err != nil {
					if !addIssue(AuditIssue{Kind: AuditInvalidValue, Key: row.Key, Detail: err.Error()}) {
						return report, nil
					}
					continue
				}
				report.Inodes++
				inodes[record.Inode] = record
				expected, err := layout.EncodeInodeKey(identity, record.Inode)
				if err != nil || !bytes.Equal(expected, row.Key) {
					if !addIssue(AuditIssue{Kind: AuditInodeKeyMismatch, Key: row.Key, Inode: record.Inode}) {
						return report, nil
					}
				}
			case layout.KeyKindDentry:
				record, err := layout.DecodeDentryValue(row.Value)
				if err != nil {
					if !addIssue(AuditIssue{Kind: AuditInvalidValue, Key: row.Key, Detail: err.Error()}) {
						return report, nil
					}
					continue
				}
				report.Dentries++
				dentries = append(dentries, record)
				refs[record.Inode]++
				expected, err := layout.EncodeDentryKey(identity, record.Parent, record.Name)
				if err != nil || !bytes.Equal(expected, row.Key) {
					if !addIssue(AuditIssue{Kind: AuditDentryKeyMismatch, Key: row.Key, Parent: record.Parent, Name: record.Name, Inode: record.Inode}) {
						return report, nil
					}
				}
			}
			next = append(append([]byte(nil), row.Key...), 0)
			advanced = true
		}
		if !advanced || uint32(len(rows)) < batchSize {
			break
		}
	}
	finishAuditReport(inodes, dentries, refs, rootInode, addIssue)
	return report, nil
}

func finishAuditReport(inodes map[model.InodeID]model.InodeRecord, dentries []model.DentryRecord, refs map[model.InodeID]uint32, rootInode model.InodeID, addIssue func(AuditIssue) bool) {
	if _, ok := inodes[rootInode]; !ok {
		if !addIssue(AuditIssue{Kind: AuditRootMissing, Inode: rootInode}) {
			return
		}
	}
	for _, dentry := range dentries {
		inode, ok := inodes[dentry.Inode]
		if !ok {
			if !addIssue(AuditIssue{Kind: AuditDentryMissingInode, Parent: dentry.Parent, Name: dentry.Name, Inode: dentry.Inode}) {
				return
			}
			continue
		}
		if inode.Type != dentry.Type {
			if !addIssue(AuditIssue{
				Kind:   AuditDentryTypeMismatch,
				Parent: dentry.Parent,
				Name:   dentry.Name,
				Inode:  dentry.Inode,
				Detail: fmt.Sprintf("dentry type=%s inode type=%s", dentry.Type, inode.Type),
			}) {
				return
			}
		}
	}
	for inodeID, inode := range inodes {
		if inodeID == rootInode {
			continue
		}
		if refs[inodeID] == 0 {
			if !addIssue(AuditIssue{Kind: AuditInodeUnreferenced, Inode: inodeID}) {
				return
			}
			continue
		}
		if refs[inodeID] != inode.LinkCount {
			if !addIssue(AuditIssue{
				Kind:   AuditLinkCountMismatch,
				Inode:  inodeID,
				Detail: fmt.Sprintf("link_count=%d refs=%d", inode.LinkCount, refs[inodeID]),
			}) {
				return
			}
		}
	}
}
