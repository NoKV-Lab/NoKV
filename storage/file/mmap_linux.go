// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/feichai0017/NoKV/storage/vfs"
	"github.com/feichai0017/NoKV/utils/mmap"
	"github.com/pkg/errors"
)

// MmapFile represents an mmapd file and includes both the buffer to the data and the file descriptor.
type MmapFile struct {
	Data  []byte
	File  vfs.File // for file ops
	fs    vfs.FS
	path  string
	fd    uintptr
	hasFD bool
}

// openMmapFileHandle maps an already-open file handle using the fd capability exposed by vfs.File.
func openMmapFileHandle(fs vfs.FS, file vfs.File, sz int, writable bool) (*MmapFile, error) {
	fs = vfs.Ensure(fs)
	filename := file.Name()
	fd, ok := vfs.FileFD(file)
	if !ok {
		return nil, errors.Errorf("unable to mmap file without fd capability: %s", filename)
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	var rerr error
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.
		if err := file.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		fileSize = int64(sz)
	}

	// fmt.Printf("Mmaping file: %s with writable: %v filesize: %d\n", fd.Name(), writable, fileSize)
	buf, err := mmap.Mmap(fd, writable, fileSize) // Mmap up to file size.
	if err != nil {
		return nil, errors.Wrapf(err, "while mmapping %s with size: %d", file.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go func() {
			_ = vfs.SyncDir(fs, dir)
		}()
	}
	return &MmapFile{
		Data:  buf,
		File:  file,
		fs:    fs,
		path:  filename,
		fd:    fd,
		hasFD: true,
	}, rerr
}

func (m *MmapFile) fileName() string {
	if m == nil {
		return ""
	}
	if m.path != "" {
		return m.path
	}
	if m.File != nil {
		return m.File.Name()
	}
	return ""
}

// SetFileName updates the logical file path associated with this mmap handle.
func (m *MmapFile) SetFileName(name string) {
	if m == nil || name == "" {
		return
	}
	m.path = name
}

// OpenMmapFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In both cases, it would mmap
// the file to maxSz and returned it. In case the file is created, z.NewFile is
// returned.
func OpenMmapFile(fs vfs.FS, filename string, flag int, maxSz int) (*MmapFile, error) {
	// fmt.Printf("opening file %s with flag: %v\n", filename, flag)
	fs = vfs.Ensure(fs)
	handle, err := fs.OpenFileHandle(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", filename)
	}
	writable := flag != os.O_RDONLY
	// if the mapped file has been opened, use its original size
	if fileInfo, err := handle.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	omf, err := openMmapFileHandle(fs, handle, maxSz, writable)
	if err != nil {
		_ = handle.Close()
		return nil, err
	}
	return omf, nil
}

type mmapReader struct {
	Data   []byte
	offset int
}

func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if m == nil || m.Data == nil || sz < 0 || off < 0 {
		return nil, io.EOF
	}
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// View returns a direct slice over the mmap'd region without copying.
// Use this only when the caller owns the mapped file lifetime.
func (m *MmapFile) View(off, sz int) ([]byte, error) {
	if m == nil || m.Data == nil || sz < 0 || off < 0 {
		return nil, io.EOF
	}
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// Slice returns the slice at the given offset.
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4
	next := start + int(sz)
	if next > len(m.Data) {
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

// AllocateSlice allocates a slice of the given size at the given offset.
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// If the file is too small, double its size or increase it by 1GB, whichever is smaller.
	if start+sz > len(m.Data) {
		const oneGB = 1 << 30
		growBy := max(min(len(m.Data), oneGB), sz+4)
		if err := m.Truncate(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

const oneGB = 1 << 30

// AppendBuffer appends data into the mmap region, growing the mapping if needed.
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
	if end > size {
		growBy := max(min(size, oneGB), needSize)
		newSize := max(size+growBy, end)
		if err := m.Truncate(int64(newSize)); err != nil {
			return err
		}
	}
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

// SyncAsync flushes dirty pages asynchronously.
func (m *MmapFile) SyncAsync() error {
	if m == nil {
		return nil
	}
	return mmap.MsyncAsync(m.Data)
}

// SyncAsyncRange flushes a range asynchronously.
func (m *MmapFile) SyncAsyncRange(off, n int64) error {
	if m == nil {
		return nil
	}
	return mmap.MsyncAsyncRange(m.Data, off, n)
}

// Remap remaps the file with the requested writability.
func (m *MmapFile) Remap(writable bool) error {
	if m == nil || !m.hasFD || m.File == nil {
		return fmt.Errorf("mmap file remap: nil receiver")
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return err
	}
	fi, err := m.File.Stat()
	if err != nil {
		return err
	}
	buf, err := mmap.Mmap(m.fd, writable, fi.Size())
	if err != nil {
		return err
	}
	m.Data = buf
	return nil
}

func (m *MmapFile) Delete() error {
	if m == nil || !m.hasFD || m.File == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.fileName(), err)
	}
	m.Data = nil
	if err := m.File.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.fileName(), err)
	}
	if err := m.File.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v", m.fileName(), err)
	}
	return vfs.Ensure(m.fs).Remove(m.fileName())
}

// Close would close the file. It would also truncate the file if maxSz >= 0.
func (m *MmapFile) Close() error {
	if m == nil || !m.hasFD || m.File == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v", m.fileName(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v", m.fileName(), err)
	}
	return m.File.Close()
}

// Truncate resizes and remaps the file to the provided size.
func (m *MmapFile) Truncate(maxSz int64) error {
	if maxSz <= 0 {
		return fmt.Errorf("invalid truncate size: %d for file: %s", maxSz, m.fileName())
	}
	if err := m.File.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v", m.fileName(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}
