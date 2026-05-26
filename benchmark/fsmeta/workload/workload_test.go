// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"bytes"
	"context"
	"testing"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestRunMDTestEasy(t *testing.T) {
	result, err := RunMDTestEasy(context.Background(), newFakeMetadataClient(), MDTestConfig{
		Mount:             "vol",
		RunID:             "easy",
		Clients:           2,
		Directories:       2,
		FilesPerDirectory: 3,
		PageLimit:         8,
	})
	require.NoError(t, err)
	require.Equal(t, MDTestEasy, result.Name)
	require.Zero(t, result.Errors)
	requireOperations(t, result, "mdtest-easy_mkdir", "mdtest-easy_create", "mdtest-easy_stat", "mdtest-easy_readdirplus", "mdtest-easy_unlink")
}

func TestRunMDTestHard(t *testing.T) {
	result, err := RunMDTestHard(context.Background(), newFakeMetadataClient(), MDTestConfig{
		Mount:             "vol",
		RunID:             "hard",
		Clients:           2,
		Directories:       4,
		FilesPerDirectory: 3,
		PageLimit:         8,
	})
	require.NoError(t, err)
	require.Equal(t, MDTestHard, result.Name)
	require.Zero(t, result.Errors)
	requireOperations(t, result, "mdtest-hard_mkdir_shared", "mdtest-hard_create", "mdtest-hard_stat", "mdtest-hard_readdirplus", "mdtest-hard_unlink")
}

func TestRunFilebenchVarmail(t *testing.T) {
	result, err := RunFilebenchVarmail(context.Background(), newFakeMetadataClient(), FilebenchVarmailConfig{
		Mount:           "vol",
		RunID:           "varmail",
		Clients:         2,
		Users:           2,
		MessagesPerUser: 2,
		PageLimit:       8,
		SessionTTL:      time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, FilebenchVarmail, result.Name)
	require.Zero(t, result.Errors)
	requireOperations(t, result,
		"filebench_varmail_mkdir_root",
		"filebench_varmail_mkdir_user",
		"filebench_varmail_create",
		"filebench_varmail_open_session",
		"filebench_varmail_update",
		"filebench_varmail_heartbeat",
		"filebench_varmail_close_session",
		"filebench_varmail_readdirplus",
		"filebench_varmail_unlink",
	)
}

func TestRunMimesisNamespace(t *testing.T) {
	result, err := RunMimesisNamespace(context.Background(), newFakeMetadataClient(), MimesisNamespaceConfig{
		Mount:             "vol",
		RunID:             "mimesis",
		Clients:           2,
		Directories:       2,
		FilesPerDirectory: 2,
		PageLimit:         8,
	})
	require.NoError(t, err)
	require.Equal(t, MimesisNamespace, result.Name)
	require.Zero(t, result.Errors)
	requireOperations(t, result,
		"mimesis_mkdir_root",
		"mimesis_mkdir",
		"mimesis_create",
		"mimesis_rename",
		"mimesis_lookup",
		"mimesis_setattr",
		"mimesis_readdirplus",
		"mimesis_unlink",
	)
}

func TestRunAICheckpointAgent(t *testing.T) {
	cli := newFakeMetadataClient()
	result, err := RunAICheckpointAgent(context.Background(), cli, AICheckpointAgentConfig{
		Mount:                   "vol",
		RunID:                   "agent",
		Clients:                 2,
		Workspaces:              2,
		CheckpointsPerWorkspace: 2,
		FilesPerCheckpoint:      2,
		PageLimit:               8,
		WatchWindow:             16,
		SessionTTL:              time.Hour,
	})
	require.NoError(t, err)
	require.Equal(t, AICheckpointAgent, result.Name)
	require.Zero(t, result.Errors)
	requireOperations(t, result,
		"ai_checkpoint_mkdir_root",
		"ai_checkpoint_mkdir_workspace",
		"ai_checkpoint_mkdir_checkpoint",
		"ai_checkpoint_create_artifact",
		"ai_checkpoint_create_manifest",
		"ai_checkpoint_open_session",
		"ai_checkpoint_update_manifest",
		"ai_checkpoint_heartbeat",
		"ai_checkpoint_close_session",
		"ai_checkpoint_publish_manifest",
		"ai_checkpoint_watch_notify",
		"ai_checkpoint_readdirplus",
		"ai_checkpoint_snapshot",
		"ai_checkpoint_snapshot_readdirplus",
		"ai_checkpoint_retire_snapshot",
	)
	cli.mu.Lock()
	snapshots := len(cli.snapshots)
	cli.mu.Unlock()
	require.Zero(t, snapshots)
}

func TestWriteSummaryCSVIncludesDriver(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSummaryCSV(&buf, []SummaryRow{{
		Workload:   MDTestEasy,
		Driver:     DriverNativeFSMetadata,
		RunID:      "run-1",
		Source:     ProfileFor(MDTestEasy).Source,
		SourceURL:  ProfileFor(MDTestEasy).SourceURL,
		Projection: ProfileFor(MDTestEasy).Projection,
		Operation:  "mdtest-easy_create",
		Count:      1,
	}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "workload,driver,run_id,source,source_url,projection,operation")
	require.Contains(t, buf.String(), "throughput_ops_sec,active_ops_per_sec,active_duration_sec")
	require.Contains(t, buf.String(), "mdtest-easy,native-fsmeta,run-1,IO500 mdtest-easy,https://io500.org/about")
}

func TestProfileForDocumentsOfficialSources(t *testing.T) {
	for _, name := range []string{MDTestEasy, MDTestHard, FilebenchVarmail, MimesisNamespace, AICheckpointAgent} {
		profile := ProfileFor(name)
		require.Equal(t, name, profile.Workload)
		require.NotEmpty(t, profile.Source)
		require.NotEmpty(t, profile.SourceURL)
		require.NotEmpty(t, profile.Shape)
		require.NotEmpty(t, profile.Projection)
	}
}

func TestScaleForLoadsOfficialProfileFile(t *testing.T) {
	require.FileExists(t, OfficialProfilePath())
	require.Equal(t, uint64(3901), mdtestFileSize(true))

	hard := ProfileFor(MDTestHard)
	require.Equal(t, "3901", hard.Official["file_size_bytes"])
	require.Equal(t, "1000000", hard.Official["n_per_rank"])
	require.Equal(t, "16000000", hard.Official["projected_total_files"])
	require.Equal(t, 16000000, ScaleFor(MDTestHard, "official").FilesPerDirectory)

	varmail := ProfileFor(FilebenchVarmail)
	require.Equal(t, "1000", varmail.Official["nfiles"])
	require.Equal(t, "16", varmail.Official["nthreads"])
	require.Equal(t, "1000", varmail.Official["projected_total_messages"])
	require.Equal(t, 1, ScaleFor(FilebenchVarmail, "official").Users)
	require.Equal(t, 1000, ScaleFor(FilebenchVarmail, "official").MessagesPerUser)

	mimesis := ProfileFor(MimesisNamespace)
	require.Equal(t, "unavailable", mimesis.Official["fixed_scale"])
	require.Equal(t, "150000000", mimesis.Official["yahoo_trace_files"])

	mlperf := ProfileFor(AICheckpointAgent)
	require.Equal(t, "10", mlperf.Official["save_operations"])
	require.Equal(t, "8, 64, 512, 1024", mlperf.Official["process_counts"])
	require.Equal(t, 8, ScaleFor(AICheckpointAgent, "official").Clients)
	require.Equal(t, 10, ScaleFor(AICheckpointAgent, "official").CheckpointsPerWorkspace)
	require.Equal(t, 1, ScaleFor(AICheckpointAgent, "official").FilesPerCheckpoint)
}

func TestTimeCallRetriesStableRetryableOperationError(t *testing.T) {
	attempts := 0
	_, err := timeCall(func() error {
		attempts++
		if attempts < 3 {
			return nokverrors.RPCStatusError(nokverrors.KindLockConflict, codes.Aborted, "live lock", nil)
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestTimeCallDoesNotRetryPermanentOrCanceledError(t *testing.T) {
	attempts := 0
	_, err := timeCall(func() error {
		attempts++
		return model.ErrNotFound
	})

	require.ErrorIs(t, err, model.ErrNotFound)
	require.Equal(t, 1, attempts)

	attempts = 0
	_, err = timeCall(func() error {
		attempts++
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func TestOpenSessionUsesFreshLeaseIDAcrossRetry(t *testing.T) {
	base := newFakeMetadataClient()
	created, err := base.Create(context.Background(), model.CreateRequest{
		Mount:  "vol",
		Parent: 1,
		Name:   "state.bin",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	cli := &retryOpenClient{fakeMetadataClient: base}
	rec := newRecorder()

	session, ok := openSession(context.Background(), cli, "vol", created.Inode.Inode, "retry-session", time.Second, "open_retry_session", rec)

	require.True(t, ok)
	require.NotEmpty(t, session)
	require.Equal(t, 2, cli.attempts)
	require.NotEqual(t, cli.firstSession, cli.secondSession)
	require.False(t, hasRecordedErrors(rec.snapshot()))
}

type retryOpenClient struct {
	*fakeMetadataClient
	attempts      int
	firstSession  model.SessionID
	secondSession model.SessionID
}

func (c *retryOpenClient) OpenWriteSession(ctx context.Context, req model.OpenWriteSessionRequest) (model.SessionRecord, error) {
	c.attempts++
	if c.attempts == 1 {
		c.firstSession = req.Session
		return model.SessionRecord{}, nokverrors.RPCStatusError(nokverrors.KindLockConflict, codes.Aborted, "live lock", nil)
	}
	c.secondSession = req.Session
	return c.fakeMetadataClient.OpenWriteSession(ctx, req)
}

func requireOperations(t *testing.T, result Result, names ...string) {
	t.Helper()
	rows := SummaryRows(result)
	ops := make(map[string]int, len(rows))
	for _, row := range rows {
		ops[row.Operation] = row.Count
	}
	for _, name := range names {
		require.Positive(t, ops[name], "missing operation %s", name)
	}
}
