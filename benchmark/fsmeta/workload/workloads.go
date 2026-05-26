// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/fsmeta/model"
)

type fileRef struct {
	parent model.InodeID
	inode  model.InodeID
	name   string
}

type checkpointTask struct {
	workspace  int
	checkpoint int
}

// RunMDTestEasy mirrors the easy mdtest shape: each worker spreads file
// creation across private directories before stat, directory scan, and remove.
func RunMDTestEasy(ctx context.Context, cli MetadataClient, cfg MDTestConfig) (Result, error) {
	cfg = normalizeMDTestConfig(cfg, MDTestEasy)
	return runMDTest(ctx, cli, cfg, MDTestEasy, false)
}

// RunMDTestHard mirrors the hard mdtest shape: all workers contend on a shared
// directory before stat, directory scan, and remove.
func RunMDTestHard(ctx context.Context, cli MetadataClient, cfg MDTestConfig) (Result, error) {
	cfg = normalizeMDTestConfig(cfg, MDTestHard)
	return runMDTest(ctx, cli, cfg, MDTestHard, true)
}

func runMDTest(ctx context.Context, cli MetadataClient, cfg MDTestConfig, name string, shared bool) (Result, error) {
	started := time.Now()
	rec := newRecorder()
	dirs, err := createMDTestDirs(ctx, cli, cfg, name, shared, rec)
	if err != nil {
		return Result{}, err
	}

	totalFiles := len(dirs) * cfg.FilesPerDirectory
	if shared {
		totalFiles = cfg.FilesPerDirectory
	}
	files := make([]fileRef, totalFiles)
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		parent, fileName := mdtestFileTarget(cfg, dirs, name, shared, idx)
		size := mdtestFileSize(shared)
		var created model.CreateResult
		if err := recordCall(rec, name+"_create", func() error {
			var err error
			created, err = cli.Create(ctx, model.CreateRequest{
				Mount:  cfg.Mount,
				Parent: parent,
				Name:   fileName,
				Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644, Size: size},
			})
			return err
		}); err != nil {
			return
		}
		files[idx] = fileRef{parent: parent, inode: created.Inode.Inode, name: fileName}
	})
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		file := files[idx]
		if file.parent == 0 {
			return
		}
		rec.recordCall(name+"_stat", func() error {
			if cli, ok := cli.(LookupPlusMetadataClient); ok {
				_, err := cli.LookupPlus(ctx, model.LookupRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
				return err
			}
			_, err := cli.Lookup(ctx, model.LookupRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
			return err
		})
	})
	runParallel(cfg.Clients, len(dirs), func(idx int) {
		rec.recordCall(name+"_readdirplus", func() error {
			_, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{Mount: cfg.Mount, Parent: dirs[idx], Limit: cfg.PageLimit})
			return err
		})
	})
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		file := files[idx]
		if file.parent == 0 {
			return
		}
		rec.recordCall(name+"_unlink", func() error {
			return cli.Unlink(ctx, model.UnlinkRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
		})
	})

	return finishResult(name, cfg.RunID, started, rec.snapshot())
}

func createMDTestDirs(ctx context.Context, cli MetadataClient, cfg MDTestConfig, workloadName string, shared bool, rec *recorder) ([]model.InodeID, error) {
	if shared {
		dir, err := createDirectory(ctx, cli, cfg.Mount, model.RootInode, fmt.Sprintf("%s-%s-shared", workloadName, cfg.RunID), workloadName+"_mkdir_shared", rec)
		if err != nil {
			return nil, err
		}
		return []model.InodeID{dir}, nil
	}
	dirs := make([]model.InodeID, cfg.Directories)
	for i := range dirs {
		dir, err := createDirectory(ctx, cli, cfg.Mount, model.RootInode, fmt.Sprintf("%s-%s-dir-%04d", workloadName, cfg.RunID, i), workloadName+"_mkdir", rec)
		if err != nil {
			return nil, err
		}
		dirs[i] = dir
	}
	return dirs, nil
}

func mdtestFileTarget(cfg MDTestConfig, dirs []model.InodeID, workloadName string, shared bool, idx int) (model.InodeID, string) {
	if shared {
		return dirs[0], fmt.Sprintf("%s-%s-file-%08d", workloadName, cfg.RunID, idx)
	}
	dir := idx % len(dirs)
	file := idx / len(dirs)
	return dirs[dir], fmt.Sprintf("%s-%s-dir-%04d-file-%08d", workloadName, cfg.RunID, dir, file)
}

func mdtestFileSize(shared bool) uint64 {
	if shared {
		return 3901
	}
	return 0
}

func RunFilebenchVarmail(ctx context.Context, cli MetadataClient, cfg FilebenchVarmailConfig) (Result, error) {
	cfg = normalizeFilebenchVarmailConfig(cfg)
	started := time.Now()
	rec := newRecorder()
	root, err := createDirectory(ctx, cli, cfg.Mount, model.RootInode, fmt.Sprintf("varmail-%s", cfg.RunID), "filebench_varmail_mkdir_root", rec)
	if err != nil {
		return Result{}, err
	}
	users := make([]model.InodeID, cfg.Users)
	for user := range users {
		dir, err := createDirectory(ctx, cli, cfg.Mount, root, fmt.Sprintf("user-%04d", user), "filebench_varmail_mkdir_user", rec)
		if err != nil {
			return Result{}, err
		}
		users[user] = dir
	}
	totalMessages := cfg.Users * cfg.MessagesPerUser
	runParallel(cfg.Clients, totalMessages, func(idx int) {
		user := idx % cfg.Users
		message := idx / cfg.Users
		runVarmailMessage(ctx, cli, cfg, users[user], user, message, rec)
	})
	runParallel(cfg.Clients, cfg.Users, func(idx int) {
		rec.recordCall("filebench_varmail_readdirplus", func() error {
			_, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{Mount: cfg.Mount, Parent: users[idx], Limit: cfg.PageLimit})
			return err
		})
	})
	runParallel(cfg.Clients, totalMessages, func(idx int) {
		user := idx % cfg.Users
		message := idx / cfg.Users
		name := varmailMessageName(user, message)
		rec.recordCall("filebench_varmail_unlink", func() error {
			return cli.Unlink(ctx, model.UnlinkRequest{Mount: cfg.Mount, Parent: users[user], Name: name})
		})
	})
	return finishResult(FilebenchVarmail, cfg.RunID, started, rec.snapshot())
}

func runVarmailMessage(ctx context.Context, cli MetadataClient, cfg FilebenchVarmailConfig, parent model.InodeID, user, message int, rec *recorder) {
	name := varmailMessageName(user, message)
	var created model.CreateResult
	if err := recordCall(rec, "filebench_varmail_create", func() error {
		var err error
		created, err = cli.Create(ctx, model.CreateRequest{
			Mount:  cfg.Mount,
			Parent: parent,
			Name:   name,
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o600, Size: uint64(4096 + message)},
		})
		return err
	}); err != nil {
		return
	}
	session, ok := openSession(ctx, cli, cfg.Mount, created.Inode.Inode, fmt.Sprintf("varmail-%s-%04d-%08d", cfg.RunID, user, message), cfg.SessionTTL, "filebench_varmail_open_session", rec)
	if !ok {
		return
	}
	rec.recordCall("filebench_varmail_update", func() error {
		_, err := cli.UpdateInode(ctx, model.UpdateInodeRequest{
			Mount:            cfg.Mount,
			Parent:           parent,
			Inode:            created.Inode.Inode,
			Name:             name,
			SetSize:          true,
			Size:             uint64(8192 + message),
			SetUpdatedUnixNs: true,
			UpdatedUnixNs:    time.Now().UnixNano(),
		})
		return err
	})
	if err := recordCall(rec, "filebench_varmail_heartbeat", func() error {
		_, err := cli.HeartbeatWriteSession(ctx, model.HeartbeatWriteSessionRequest{
			Mount:   cfg.Mount,
			Inode:   created.Inode.Inode,
			Session: session,
			TTL:     2 * cfg.SessionTTL,
		})
		return err
	}); err != nil {
		return
	}
	rec.recordCall("filebench_varmail_close_session", func() error {
		return cli.CloseWriteSession(ctx, model.CloseWriteSessionRequest{Mount: cfg.Mount, Inode: created.Inode.Inode, Session: session})
	})
}

func varmailMessageName(user, message int) string {
	return fmt.Sprintf("msg-user-%04d-%08d.eml", user, message)
}

func RunMimesisNamespace(ctx context.Context, cli MetadataClient, cfg MimesisNamespaceConfig) (Result, error) {
	cfg = normalizeMimesisNamespaceConfig(cfg)
	started := time.Now()
	rec := newRecorder()
	root, err := createDirectory(ctx, cli, cfg.Mount, model.RootInode, fmt.Sprintf("mimesis-%s", cfg.RunID), "mimesis_mkdir_root", rec)
	if err != nil {
		return Result{}, err
	}
	dirs := make([]model.InodeID, cfg.Directories)
	for i := range dirs {
		dir, err := createDirectory(ctx, cli, cfg.Mount, root, fmt.Sprintf("dir-%04d", i), "mimesis_mkdir", rec)
		if err != nil {
			return Result{}, err
		}
		dirs[i] = dir
	}
	totalFiles := cfg.Directories * cfg.FilesPerDirectory
	files := make([]fileRef, totalFiles)
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		dir := idx % cfg.Directories
		file := idx / cfg.Directories
		name := fmt.Sprintf("mimesis-%s-dir-%04d-file-%08d.dat", cfg.RunID, dir, file)
		var created model.CreateResult
		if err := recordCall(rec, "mimesis_create", func() error {
			var err error
			created, err = cli.Create(ctx, model.CreateRequest{
				Mount:  cfg.Mount,
				Parent: dirs[dir],
				Name:   name,
				Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644, Size: uint64(1024 + file)},
			})
			return err
		}); err != nil {
			return
		}
		files[idx] = fileRef{parent: dirs[dir], inode: created.Inode.Inode, name: name}
	})
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		file := files[idx]
		if file.parent == 0 {
			return
		}
		renamed := file.name + ".renamed"
		if err := recordCall(rec, "mimesis_rename", func() error {
			return cli.Rename(ctx, model.RenameRequest{
				Mount:      cfg.Mount,
				FromParent: file.parent,
				FromName:   file.name,
				ToParent:   file.parent,
				ToName:     renamed,
			})
		}); err != nil {
			return
		}
		file.name = renamed
		files[idx] = file
		rec.recordCall("mimesis_lookup", func() error {
			if cli, ok := cli.(LookupPlusMetadataClient); ok {
				_, err := cli.LookupPlus(ctx, model.LookupRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
				return err
			}
			_, err := cli.Lookup(ctx, model.LookupRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
			return err
		})
		rec.recordCall("mimesis_setattr", func() error {
			_, err := cli.UpdateInode(ctx, model.UpdateInodeRequest{
				Mount:            cfg.Mount,
				Parent:           file.parent,
				Inode:            file.inode,
				Name:             file.name,
				SetMode:          true,
				Mode:             0o640,
				SetUpdatedUnixNs: true,
				UpdatedUnixNs:    time.Now().UnixNano(),
			})
			return err
		})
	})
	runParallel(cfg.Clients, len(dirs), func(idx int) {
		rec.recordCall("mimesis_readdirplus", func() error {
			_, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{Mount: cfg.Mount, Parent: dirs[idx], Limit: cfg.PageLimit})
			return err
		})
	})
	runParallel(cfg.Clients, totalFiles, func(idx int) {
		file := files[idx]
		if file.parent == 0 {
			return
		}
		rec.recordCall("mimesis_unlink", func() error {
			return cli.Unlink(ctx, model.UnlinkRequest{Mount: cfg.Mount, Parent: file.parent, Name: file.name})
		})
	})
	return finishResult(MimesisNamespace, cfg.RunID, started, rec.snapshot())
}

func RunAICheckpointAgent(ctx context.Context, cli MetadataClient, cfg AICheckpointAgentConfig) (Result, error) {
	cfg = normalizeAICheckpointAgentConfig(cfg)
	started := time.Now()
	rec := newRecorder()
	root, err := createDirectory(ctx, cli, cfg.Mount, model.RootInode, fmt.Sprintf("ai-agent-%s", cfg.RunID), "ai_checkpoint_mkdir_root", rec)
	if err != nil {
		return Result{}, err
	}
	workspaceRoot, err := createDirectory(ctx, cli, cfg.Mount, root, "workspaces", "ai_checkpoint_mkdir_workspace_root", rec)
	if err != nil {
		return Result{}, err
	}
	publishedRoot, err := createDirectory(ctx, cli, cfg.Mount, root, "published", "ai_checkpoint_mkdir_publish_root", rec)
	if err != nil {
		return Result{}, err
	}
	checkpointParents := make([]model.InodeID, cfg.Workspaces)
	for workspace := range checkpointParents {
		workspaceDir, err := createDirectory(ctx, cli, cfg.Mount, workspaceRoot, fmt.Sprintf("workspace-%04d", workspace), "ai_checkpoint_mkdir_workspace", rec)
		if err != nil {
			return Result{}, err
		}
		parent, err := createDirectory(ctx, cli, cfg.Mount, workspaceDir, "checkpoints", "ai_checkpoint_mkdir_checkpoint_root", rec)
		if err != nil {
			return Result{}, err
		}
		checkpointParents[workspace] = parent
	}

	stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		Mount:              cfg.Mount,
		RootInode:          publishedRoot,
		BackPressureWindow: cfg.WatchWindow,
	})
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = stream.Close() }()
	if _, err := cli.Create(ctx, model.CreateRequest{
		Mount:  cfg.Mount,
		Parent: publishedRoot,
		Name:   "watch-warmup",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644},
	}); err != nil {
		return Result{}, err
	}
	if err := waitForWatchName(ctx, stream, "watch-warmup"); err != nil {
		return Result{}, err
	}

	starts := newWatchStarts()
	var successfulPublishes atomic.Int64
	var deliveredPublishes atomic.Int64
	publishesDone := make(chan struct{})
	watchDone := make(chan error, 1)
	go collectWatchEvents(ctx, stream, starts, &successfulPublishes, &deliveredPublishes, publishesDone, "ai_checkpoint_watch_notify", rec, watchDone)

	totalCheckpoints := cfg.Workspaces * cfg.CheckpointsPerWorkspace
	runParallel(cfg.Clients, totalCheckpoints, func(idx int) {
		task := checkpointTask{workspace: idx % cfg.Workspaces, checkpoint: idx / cfg.Workspaces}
		runAICheckpointTask(ctx, cli, cfg, checkpointParents[task.workspace], publishedRoot, task, starts, &successfulPublishes, rec)
	})
	close(publishesDone)
	if hasRecordedErrors(rec.snapshot()) {
		_ = stream.Close()
	}
	closeWatchAfterDelivery(stream, &successfulPublishes, &deliveredPublishes)
	if watchErr := <-watchDone; watchErr != nil {
		rec.record("ai_checkpoint_watch_notify", 0, watchErr)
	}
	return finishResult(AICheckpointAgent, cfg.RunID, started, rec.snapshot())
}

func runAICheckpointTask(ctx context.Context, cli MetadataClient, cfg AICheckpointAgentConfig, workspaceParent, publishedRoot model.InodeID, task checkpointTask, starts *watchStarts, successfulPublishes *atomic.Int64, rec *recorder) {
	checkpointName := fmt.Sprintf("workspace-%04d-checkpoint-%08d", task.workspace, task.checkpoint)
	checkpointDir, err := createDirectory(ctx, cli, cfg.Mount, workspaceParent, checkpointName, "ai_checkpoint_mkdir_checkpoint", rec)
	if err != nil {
		return
	}
	for file := 0; file < cfg.FilesPerCheckpoint; file++ {
		file := file
		rec.recordCall("ai_checkpoint_create_artifact", func() error {
			_, err := cli.Create(ctx, model.CreateRequest{
				Mount:  cfg.Mount,
				Parent: checkpointDir,
				Name:   fmt.Sprintf("shard-%05d.bin", file),
				Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644, Size: uint64(64<<20 + file)},
			})
			return err
		})
	}
	var manifest model.CreateResult
	if err := recordCall(rec, "ai_checkpoint_create_manifest", func() error {
		var err error
		manifest, err = cli.Create(ctx, model.CreateRequest{
			Mount:  cfg.Mount,
			Parent: checkpointDir,
			Name:   "manifest.tmp",
			Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Mode: 0o644, Size: 4096},
		})
		return err
	}); err != nil {
		return
	}
	session, ok := openSession(ctx, cli, cfg.Mount, manifest.Inode.Inode, fmt.Sprintf("ai-%s-%04d-%08d", cfg.RunID, task.workspace, task.checkpoint), cfg.SessionTTL, "ai_checkpoint_open_session", rec)
	if !ok {
		return
	}
	rec.recordCall("ai_checkpoint_update_manifest", func() error {
		_, err := cli.UpdateInode(ctx, model.UpdateInodeRequest{
			Mount:            cfg.Mount,
			Parent:           checkpointDir,
			Inode:            manifest.Inode.Inode,
			Name:             "manifest.tmp",
			SetSize:          true,
			Size:             uint64(8192 + task.checkpoint),
			SetUpdatedUnixNs: true,
			UpdatedUnixNs:    time.Now().UnixNano(),
		})
		return err
	})
	if err := recordCall(rec, "ai_checkpoint_heartbeat", func() error {
		_, err := cli.HeartbeatWriteSession(ctx, model.HeartbeatWriteSessionRequest{
			Mount:   cfg.Mount,
			Inode:   manifest.Inode.Inode,
			Session: session,
			TTL:     2 * cfg.SessionTTL,
		})
		return err
	}); err != nil {
		return
	}
	rec.recordCall("ai_checkpoint_close_session", func() error {
		return cli.CloseWriteSession(ctx, model.CloseWriteSessionRequest{Mount: cfg.Mount, Inode: manifest.Inode.Inode, Session: session})
	})
	finalName := checkpointName + ".manifest.json"
	if err := recordCall(rec, "ai_checkpoint_publish_manifest", func() error {
		starts.put(finalName, time.Now())
		return cli.Rename(ctx, model.RenameRequest{
			Mount:      cfg.Mount,
			FromParent: checkpointDir,
			FromName:   "manifest.tmp",
			ToParent:   publishedRoot,
			ToName:     finalName,
		})
	}); err != nil {
		starts.delete(finalName)
		return
	}
	successfulPublishes.Add(1)
	rec.recordCall("ai_checkpoint_readdirplus", func() error {
		_, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{Mount: cfg.Mount, Parent: checkpointDir, Limit: cfg.PageLimit})
		return err
	})
	var token model.SnapshotSubtreeToken
	if err := recordCall(rec, "ai_checkpoint_snapshot", func() error {
		var err error
		token, err = cli.SnapshotSubtree(ctx, model.SnapshotSubtreeRequest{Mount: cfg.Mount, RootInode: checkpointDir})
		return err
	}); err != nil {
		return
	}
	rec.recordCall("ai_checkpoint_snapshot_readdirplus", func() error {
		_, err := cli.ReadDirPlus(ctx, model.ReadDirRequest{
			Mount:           cfg.Mount,
			Parent:          checkpointDir,
			Limit:           cfg.PageLimit,
			SnapshotVersion: token.ReadVersion,
		})
		return err
	})
	rec.recordCall("ai_checkpoint_retire_snapshot", func() error {
		return cli.RetireSnapshotSubtree(ctx, token)
	})
}

func createDirectory(ctx context.Context, cli MetadataClient, mount model.MountID, parent model.InodeID, name, op string, rec *recorder) (model.InodeID, error) {
	var inode model.InodeID
	err := recordCall(rec, op, func() error {
		result, err := cli.Create(ctx, model.CreateRequest{
			Mount:  mount,
			Parent: parent,
			Name:   name,
			Attrs:  model.CreateAttrs{Type: model.InodeTypeDirectory, Mode: 0o755},
		})
		if err == nil {
			inode = result.Inode.Inode
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	if inode == 0 {
		return 0, fmt.Errorf("%s did not create inode for %q", op, name)
	}
	return inode, nil
}

func openSession(ctx context.Context, cli MetadataClient, mount model.MountID, inode model.InodeID, base string, ttl time.Duration, op string, rec *recorder) (model.SessionID, bool) {
	var session model.SessionID
	attempt := 0
	err := recordCall(rec, op, func() error {
		attempt++
		candidate := model.SessionID(fmt.Sprintf("%s-attempt-%02d", base, attempt))
		_, err := cli.OpenWriteSession(ctx, model.OpenWriteSessionRequest{
			Mount:   mount,
			Inode:   inode,
			Session: candidate,
			TTL:     ttl,
		})
		if err == nil {
			session = candidate
		}
		return err
	})
	return session, err == nil && session != ""
}

func normalizeMDTestConfig(cfg MDTestConfig, name string) MDTestConfig {
	scale := ScaleFor(name, DefaultScaleProfile)
	cfg.Mount = defaultMount(cfg.Mount)
	cfg.RunID = defaultRunID(cfg.RunID)
	cfg.Clients = defaultInt(cfg.Clients, scale.Clients, 4)
	if cfg.Directories <= 0 {
		cfg.Directories = defaultInt(0, scale.Directories, 16)
	}
	if cfg.FilesPerDirectory <= 0 {
		cfg.FilesPerDirectory = defaultInt(0, scale.FilesPerDirectory, 128)
	}
	cfg.PageLimit = clampReadDirLimit(defaultUint32(cfg.PageLimit, scale.PageLimit, 0), cfg.FilesPerDirectory)
	return cfg
}

func normalizeFilebenchVarmailConfig(cfg FilebenchVarmailConfig) FilebenchVarmailConfig {
	scale := ScaleFor(FilebenchVarmail, DefaultScaleProfile)
	cfg.Mount = defaultMount(cfg.Mount)
	cfg.RunID = defaultRunID(cfg.RunID)
	cfg.Clients = defaultInt(cfg.Clients, scale.Clients, 4)
	if cfg.Users <= 0 {
		cfg.Users = defaultInt(0, scale.Users, 16)
	}
	if cfg.MessagesPerUser <= 0 {
		cfg.MessagesPerUser = defaultInt(0, scale.MessagesPerUser, 128)
	}
	cfg.PageLimit = clampReadDirLimit(defaultUint32(cfg.PageLimit, scale.PageLimit, 0), cfg.MessagesPerUser)
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = scale.SessionTTLDuration(10 * time.Second)
	}
	return cfg
}

func normalizeMimesisNamespaceConfig(cfg MimesisNamespaceConfig) MimesisNamespaceConfig {
	scale := ScaleFor(MimesisNamespace, DefaultScaleProfile)
	cfg.Mount = defaultMount(cfg.Mount)
	cfg.RunID = defaultRunID(cfg.RunID)
	cfg.Clients = defaultInt(cfg.Clients, scale.Clients, 4)
	if cfg.Directories <= 0 {
		cfg.Directories = defaultInt(0, scale.Directories, 32)
	}
	if cfg.FilesPerDirectory <= 0 {
		cfg.FilesPerDirectory = defaultInt(0, scale.FilesPerDirectory, 128)
	}
	cfg.PageLimit = clampReadDirLimit(defaultUint32(cfg.PageLimit, scale.PageLimit, 0), cfg.FilesPerDirectory)
	return cfg
}

func normalizeAICheckpointAgentConfig(cfg AICheckpointAgentConfig) AICheckpointAgentConfig {
	scale := ScaleFor(AICheckpointAgent, DefaultScaleProfile)
	cfg.Mount = defaultMount(cfg.Mount)
	cfg.RunID = defaultRunID(cfg.RunID)
	cfg.Clients = defaultInt(cfg.Clients, scale.Clients, 4)
	if cfg.Workspaces <= 0 {
		cfg.Workspaces = defaultInt(0, scale.Workspaces, 8)
	}
	if cfg.CheckpointsPerWorkspace <= 0 {
		cfg.CheckpointsPerWorkspace = defaultInt(0, scale.CheckpointsPerWorkspace, 64)
	}
	if cfg.FilesPerCheckpoint <= 0 {
		cfg.FilesPerCheckpoint = defaultInt(0, scale.FilesPerCheckpoint, 8)
	}
	cfg.PageLimit = clampReadDirLimit(defaultUint32(cfg.PageLimit, scale.PageLimit, 0), cfg.FilesPerCheckpoint+1)
	if cfg.WatchWindow == 0 {
		cfg.WatchWindow = defaultUint32(0, scale.WatchWindow, uint32(cfg.Workspaces*cfg.CheckpointsPerWorkspace+1))
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = scale.SessionTTLDuration(10 * time.Second)
	}
	return cfg
}
