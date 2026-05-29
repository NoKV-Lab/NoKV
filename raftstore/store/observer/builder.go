// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package observer

import (
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	raftcmdpb "github.com/feichai0017/NoKV/pb/raft"
	myraft "github.com/feichai0017/NoKV/raft"
	kv "github.com/feichai0017/NoKV/txn/storage"
)

// EventsFromCommand inspects one applied raft command (entry + paired
// request/response) and returns the post-apply Events that should be
// published to observers. Errored sub-responses and atomic-mutate
// fallbacks contribute no events.
func EventsFromCommand(entry myraft.Entry, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) []Event {
	if req == nil || resp == nil {
		return nil
	}
	regionID := req.GetHeader().GetRegionId()
	responses := resp.GetResponses()
	var out []Event
	for i, request := range req.GetRequests() {
		if request == nil {
			continue
		}
		var response *raftcmdpb.Response
		if i < len(responses) {
			response = responses[i]
		}
		switch request.GetCmdType() {
		case raftcmdpb.CmdType_CMD_COMMIT:
			commit := request.GetCommit()
			if commit == nil || len(commit.GetKeys()) == 0 || response == nil || response.GetCommit() == nil {
				continue
			}
			if response.GetCommit().GetError() != nil {
				continue
			}
			out = append(out, Event{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        SourceCommit,
				CommitVersion: commit.GetCommitVersion(),
				Keys:          cloneKeys(commit.GetKeys()),
			})
		case raftcmdpb.CmdType_CMD_RESOLVE_LOCK:
			resolve := request.GetResolveLock()
			if resolve == nil || resolve.GetCommitVersion() == 0 || len(resolve.GetKeys()) == 0 || response == nil || response.GetResolveLock() == nil {
				continue
			}
			if response.GetResolveLock().GetError() != nil {
				continue
			}
			out = append(out, Event{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        SourceResolveLock,
				CommitVersion: resolve.GetCommitVersion(),
				Keys:          cloneKeys(resolve.GetKeys()),
			})
		case raftcmdpb.CmdType_CMD_TRY_ATOMIC_MUTATE:
			atomicMutate := request.GetTryAtomicMutate()
			if atomicMutate == nil || atomicMutate.GetCommitVersion() == 0 || len(atomicMutate.GetMutations()) == 0 || response == nil || response.GetTryAtomicMutate() == nil {
				continue
			}
			if response.GetTryAtomicMutate().GetError() != nil || response.GetTryAtomicMutate().GetFallbackToTwoPhaseCommit() {
				continue
			}
			out = append(out, Event{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        SourceCommit,
				CommitVersion: atomicMutate.GetCommitVersion(),
				Keys:          cloneMutationKeys(atomicMutate.GetMutations()),
				AtomicMutate:  true,
			})
		case raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC:
			install := request.GetInstallPreparedMvcc()
			if install == nil || install.GetCommitVersion() == 0 || len(install.GetWatchKeys()) == 0 || response == nil || response.GetInstallPreparedMvcc() == nil {
				continue
			}
			if response.GetInstallPreparedMvcc().GetError() != nil {
				continue
			}
			out = append(out, Event{
				RegionID:      regionID,
				Term:          entry.Term,
				Index:         entry.Index,
				Source:        SourceCommit,
				CommitVersion: install.GetCommitVersion(),
				Keys:          cloneKeys(install.GetWatchKeys()),
			})
		}
	}
	return out
}

// AttachCommandCursor copies the raft apply cursor into response payloads that
// need to publish from the submitter process after apply completes.
func AttachCommandCursor(entry myraft.Entry, req *raftcmdpb.RaftCmdRequest, resp *raftcmdpb.RaftCmdResponse) {
	if req == nil || resp == nil {
		return
	}
	regionID := req.GetHeader().GetRegionId()
	responses := resp.GetResponses()
	for i, request := range req.GetRequests() {
		if request == nil || request.GetCmdType() != raftcmdpb.CmdType_CMD_INSTALL_PREPARED_MVCC {
			continue
		}
		install := request.GetInstallPreparedMvcc()
		if install == nil {
			continue
		}
		if i >= len(responses) || responses[i] == nil {
			continue
		}
		prepared := responses[i].GetInstallPreparedMvcc()
		if prepared == nil || prepared.GetError() != nil {
			continue
		}
		prepared.RegionId = regionID
		prepared.Term = entry.Term
		prepared.Index = entry.Index
		prepared.CommitVersion = install.GetCommitVersion()
	}
}

func cloneKeys(keys [][]byte) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 0 {
			out = append(out, nil)
			continue
		}
		out = append(out, kv.SafeCopy(nil, key))
	}
	return out
}

func cloneMutationKeys(mutations []*kvrpcpb.Mutation) [][]byte {
	if len(mutations) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(mutations))
	for _, mut := range mutations {
		if mut == nil {
			continue
		}
		out = append(out, kv.SafeCopy(nil, mut.GetKey()))
	}
	return out
}
