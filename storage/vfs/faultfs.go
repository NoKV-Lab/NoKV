// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package vfs

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Op identifies a filesystem operation for failure injection.
type Op string

const (
	OpOpen      Op = "open"
	OpOpenFile  Op = "open_file"
	OpLock      Op = "lock"
	OpFileWrite Op = "file_write"
	OpFileSync  Op = "file_sync"
	OpFileClose Op = "file_close"
	OpFileTrunc Op = "file_truncate"
	OpMkdirAll  Op = "mkdir_all"
	OpRemoveAll Op = "remove_all"
	OpRemove    Op = "remove"
	OpRename    Op = "rename"
	OpStat      Op = "stat"
	OpReadDir   Op = "read_dir"
	OpReadFile  Op = "read_file"
	OpWriteFile Op = "write_file"
	OpTruncate  Op = "truncate"
	OpGlob      Op = "glob"
	OpHostname  Op = "hostname"
)

// Hook is invoked before each filesystem operation.
// Returning a non-nil error simulates an operation failure.
type Hook func(op Op, path string) error

// FaultRule describes one fault-injection rule for FaultFS.
// A rule matches by operation and optional exact path, then starts injecting
// errors at TriggerAt (1-based) and keeps injecting until MaxFailures is
// reached. MaxFailures=0 means unlimited after TriggerAt.
type FaultRule struct {
	Op          Op
	Path        string
	SrcPath     string
	DstPath     string
	Err         error
	TriggerAt   uint64
	MaxFailures uint64

	matched uint64
	failed  uint64
}

// FaultPolicy evaluates a list of fault-injection rules in order.
// The first rule that injects an error stops evaluation for that operation.
type FaultPolicy struct {
	mu    sync.Mutex
	hook  Hook
	rules []FaultRule
}

// NewFaultPolicy builds a policy from the provided rules.
func NewFaultPolicy(rules ...FaultRule) *FaultPolicy {
	p := &FaultPolicy{
		rules: make([]FaultRule, 0, len(rules)),
	}
	for _, rule := range rules {
		p.AddRule(rule)
	}
	return p
}

// SetHook installs a hook callback into the policy. Nil disables hook-based
// injection while retaining rule-based behavior.
func (p *FaultPolicy) SetHook(hook Hook) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.hook = hook
	p.mu.Unlock()
}

// AddRule appends a rule to the policy.
func (p *FaultPolicy) AddRule(rule FaultRule) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, normalizeFaultRule(rule))
}

// Hook evaluates policy rules and returns an injected error when matched.
// Hook satisfies the Hook function contract and can be passed to NewFaultFS.
func (p *FaultPolicy) Hook(op Op, path string) error {
	return p.inject(op, path, "", "")
}

func (p *FaultPolicy) inject(op Op, path, renameSrc, renameDst string) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	hook := p.hook
	p.mu.Unlock()
	if hook != nil {
		hookPath := path
		if op == OpRename {
			hookPath = renameSrc + "->" + renameDst
		}
		if err := hook(op, hookPath); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rules {
		if err := p.rules[i].evaluate(op, path, renameSrc, renameDst); err != nil {
			return err
		}
	}
	return nil
}

// FailOnceRule creates a rule that fails exactly once on the first match.
func FailOnceRule(op Op, path string, err error) FaultRule {
	return FaultRule{
		Op:          op,
		Path:        path,
		Err:         err,
		TriggerAt:   1,
		MaxFailures: 1,
	}
}

// FailOnNthRule creates a rule that fails exactly once on the Nth match.
// nth<=1 is treated as 1.
func FailOnNthRule(op Op, path string, nth uint64, err error) FaultRule {
	if nth == 0 {
		nth = 1
	}
	return FaultRule{
		Op:          op,
		Path:        path,
		Err:         err,
		TriggerAt:   nth,
		MaxFailures: 1,
	}
}

// FailAfterNthRule creates a rule that starts failing from the Nth match and
// keeps failing on every subsequent match. nth<=1 is treated as 1.
func FailAfterNthRule(op Op, path string, nth uint64, err error) FaultRule {
	if nth == 0 {
		nth = 1
	}
	return FaultRule{
		Op:        op,
		Path:      path,
		Err:       err,
		TriggerAt: nth,
	}
}

// FailOnceRenameRule creates a rename-specific rule that fails exactly once.
// src or dst can be empty to match either side.
func FailOnceRenameRule(src, dst string, err error) FaultRule {
	return FaultRule{
		Op:          OpRename,
		SrcPath:     src,
		DstPath:     dst,
		Err:         err,
		TriggerAt:   1,
		MaxFailures: 1,
	}
}

// NewFaultFSWithPolicy returns a FaultFS configured from a rule policy.
func NewFaultFSWithPolicy(base FS, policy *FaultPolicy) *FaultFS {
	if policy == nil {
		return NewFaultFS(base, nil)
	}
	return &FaultFS{
		base:   Ensure(base),
		policy: policy,
	}
}

// FaultFS decorates an FS and injects failures via Hook.
type FaultFS struct {
	base   FS
	policy *FaultPolicy
}

// NewFaultFS returns an FS wrapper that can inject operation failures.
func NewFaultFS(base FS, hook Hook) *FaultFS {
	policy := NewFaultPolicy()
	policy.SetHook(hook)
	return &FaultFS{
		base:   Ensure(base),
		policy: policy,
	}
}

func (f *FaultFS) before(op Op, path string) error {
	if f == nil {
		return nil
	}
	if f.policy == nil {
		return nil
	}
	return f.policy.inject(op, path, "", "")
}

func (f *FaultFS) beforeRename(oldPath, newPath string) error {
	if f == nil {
		return nil
	}
	if f.policy == nil {
		return nil
	}
	return f.policy.inject(OpRename, oldPath, oldPath, newPath)
}

func normalizeFaultRule(rule FaultRule) FaultRule {
	if rule.TriggerAt == 0 {
		rule.TriggerAt = 1
	}
	return rule
}

func (r *FaultRule) evaluate(op Op, path, renameSrc, renameDst string) error {
	if !r.matches(op, path, renameSrc, renameDst) {
		return nil
	}
	r.matched++
	if r.matched < r.TriggerAt {
		return nil
	}
	if r.MaxFailures > 0 && r.failed >= r.MaxFailures {
		return nil
	}
	r.failed++
	if r.Err != nil {
		return r.Err
	}
	return fmt.Errorf("faultfs injected failure: op=%s path=%s", op, path)
}

func (r *FaultRule) matches(op Op, path, renameSrc, renameDst string) bool {
	if r.Op != "" && r.Op != op {
		return false
	}
	if op == OpRename {
		if r.Path != "" {
			return false
		}
		if r.SrcPath != "" && r.SrcPath != renameSrc {
			return false
		}
		if r.DstPath != "" && r.DstPath != renameDst {
			return false
		}
		return true
	}
	if r.Path != "" && r.Path != path {
		return false
	}
	return true
}

// OpenHandle opens an existing file for reading and returns a vfs.File.
func (f *FaultFS) OpenHandle(name string) (File, error) {
	if err := f.before(OpOpen, name); err != nil {
		return nil, err
	}
	file, err := f.base.OpenHandle(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{base: file, parent: f, path: name}, nil
}

// OpenFileHandle opens or creates a file and returns a vfs.File.
func (f *FaultFS) OpenFileHandle(name string, flag int, perm os.FileMode) (File, error) {
	if err := f.before(OpOpenFile, name); err != nil {
		return nil, err
	}
	file, err := f.base.OpenFileHandle(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{base: file, parent: f, path: name}, nil
}

// Lock acquires an primacy lock on name.
func (f *FaultFS) Lock(name string) (io.Closer, error) {
	if err := f.before(OpLock, name); err != nil {
		return nil, err
	}
	return f.base.Lock(name)
}

// MkdirAll creates a directory hierarchy.
func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	if err := f.before(OpMkdirAll, path); err != nil {
		return err
	}
	return f.base.MkdirAll(path, perm)
}

// RemoveAll removes a path recursively.
func (f *FaultFS) RemoveAll(path string) error {
	if err := f.before(OpRemoveAll, path); err != nil {
		return err
	}
	return f.base.RemoveAll(path)
}

// Remove removes a file or empty directory.
func (f *FaultFS) Remove(name string) error {
	if err := f.before(OpRemove, name); err != nil {
		return err
	}
	return f.base.Remove(name)
}

// Rename renames (moves) a file or directory.
func (f *FaultFS) Rename(oldPath, newPath string) error {
	if err := f.beforeRename(oldPath, newPath); err != nil {
		return err
	}
	return f.base.Rename(oldPath, newPath)
}

// RenameNoReplace renames oldPath to newPath without replacing an existing target.
func (f *FaultFS) RenameNoReplace(oldPath, newPath string) error {
	if err := f.beforeRename(oldPath, newPath); err != nil {
		return err
	}
	return f.base.RenameNoReplace(oldPath, newPath)
}

// Stat returns file metadata.
func (f *FaultFS) Stat(name string) (os.FileInfo, error) {
	if err := f.before(OpStat, name); err != nil {
		return nil, err
	}
	return f.base.Stat(name)
}

// ReadDir lists directory entries.
func (f *FaultFS) ReadDir(name string) ([]os.DirEntry, error) {
	if err := f.before(OpReadDir, name); err != nil {
		return nil, err
	}
	return f.base.ReadDir(name)
}

// ReadFile reads an entire file.
func (f *FaultFS) ReadFile(name string) ([]byte, error) {
	if err := f.before(OpReadFile, name); err != nil {
		return nil, err
	}
	return f.base.ReadFile(name)
}

// WriteFile writes an entire file.
func (f *FaultFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if err := f.before(OpWriteFile, name); err != nil {
		return err
	}
	return f.base.WriteFile(name, data, perm)
}

// Truncate resizes a file.
func (f *FaultFS) Truncate(name string, size int64) error {
	if err := f.before(OpTruncate, name); err != nil {
		return err
	}
	return f.base.Truncate(name, size)
}

// Glob expands filesystem patterns.
func (f *FaultFS) Glob(pattern string) ([]string, error) {
	if err := f.before(OpGlob, pattern); err != nil {
		return nil, err
	}
	return f.base.Glob(pattern)
}

// Hostname returns local hostname.
func (f *FaultFS) Hostname() (string, error) {
	if err := f.before(OpHostname, ""); err != nil {
		return "", err
	}
	return f.base.Hostname()
}

type faultFile struct {
	base   File
	parent *FaultFS
	path   string
}

func (f *faultFile) before(op Op) error {
	if f == nil || f.parent == nil {
		return nil
	}
	return f.parent.before(op, f.path)
}

func (f *faultFile) Read(p []byte) (int, error) {
	return f.base.Read(p)
}

// forwarding-ok: faultFile satisfies io.ReaderAt; injection is gated by before/after hooks elsewhere.
func (f *faultFile) ReadAt(p []byte, off int64) (int, error) {
	return f.base.ReadAt(p, off)
}

func (f *faultFile) Write(p []byte) (int, error) {
	if err := f.before(OpFileWrite); err != nil {
		return 0, err
	}
	return f.base.Write(p)
}

func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.before(OpFileWrite); err != nil {
		return 0, err
	}
	return f.base.WriteAt(p, off)
}

func (f *faultFile) Seek(offset int64, whence int) (int64, error) {
	return f.base.Seek(offset, whence)
}

func (f *faultFile) Close() error {
	if err := f.before(OpFileClose); err != nil {
		return err
	}
	return f.base.Close()
}

func (f *faultFile) Stat() (os.FileInfo, error) {
	return f.base.Stat()
}

func (f *faultFile) Sync() error {
	if err := f.before(OpFileSync); err != nil {
		return err
	}
	return f.base.Sync()
}

func (f *faultFile) Truncate(size int64) error {
	if err := f.before(OpFileTrunc); err != nil {
		return err
	}
	return f.base.Truncate(size)
}

func (f *faultFile) Name() string {
	if f.path != "" {
		return f.path
	}
	return f.base.Name()
}

func (f *faultFile) Fd() uintptr {
	if fd, ok := FileFD(f.base); ok {
		return fd
	}
	return 0
}

func (f *faultFile) OSFile() *os.File {
	if of, ok := UnwrapOSFile(f.base); ok {
		return of
	}
	return nil
}
