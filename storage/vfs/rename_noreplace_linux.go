// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package vfs

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplaceOS(oldPath, newPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EEXIST) {
		return os.ErrExist
	}
	// Some kernels/filesystems do not support atomic no-replace rename.
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
		return fmt.Errorf("%w: %v", ErrRenameNoReplaceUnsupported, err)
	}
	return err
}
