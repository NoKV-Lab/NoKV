/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Explicit single-artifact transfer helpers for local executables.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub fn read_collect_source(source: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        format!(
            "cannot inspect collect source {}: {error}",
            source.display()
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!(
            "collect source {} is a symlink; symlinks are not followed",
            source.display()
        ));
    }
    if !metadata.is_file() {
        return Err(format!(
            "collect source {} is not a regular file",
            source.display()
        ));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(format!(
            "collect source {} is {} bytes, maximum is {max_bytes}",
            source.display(),
            metadata.len()
        ));
    }
    let bytes = fs::read(source)
        .map_err(|error| format!("cannot read collect source {}: {error}", source.display()))?;
    if bytes.len() > max_bytes {
        return Err(format!(
            "collect source {} grew to {} bytes, maximum is {max_bytes}",
            source.display(),
            bytes.len()
        ));
    }
    Ok(bytes)
}

pub fn write_materialized_file(destination: &Path, bytes: &[u8]) -> Result<PathBuf, String> {
    let parent = destination.parent().ok_or_else(|| {
        format!(
            "materialize destination {} has no parent directory",
            destination.display()
        )
    })?;
    let parent = parent.canonicalize().map_err(|error| {
        format!(
            "cannot canonicalize materialize parent {}: {error}",
            parent.display()
        )
    })?;
    if !parent.is_dir() {
        return Err(format!(
            "materialize parent {} is not a directory",
            parent.display()
        ));
    }
    let file_name = destination.file_name().ok_or_else(|| {
        format!(
            "materialize destination {} has no file name",
            destination.display()
        )
    })?;
    let destination = parent.join(file_name);
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&destination)
        .map_err(|error| {
            format!(
                "cannot create materialize destination {} without overwriting: {error}",
                destination.display()
            )
        })?;
    output.write_all(bytes).map_err(|error| {
        format!(
            "cannot write materialize destination {}: {error}",
            destination.display()
        )
    })?;
    output.sync_all().map_err(|error| {
        format!(
            "cannot sync materialize destination {}: {error}",
            destination.display()
        )
    })?;
    Ok(destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_reads_only_bounded_regular_files() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("result.bin");
        fs::write(&source, b"result").unwrap();
        assert_eq!(read_collect_source(&source, 6).unwrap(), b"result");
        assert!(read_collect_source(&source, 5)
            .unwrap_err()
            .contains("maximum"));
        assert!(read_collect_source(root.path(), 1024)
            .unwrap_err()
            .contains("regular file"));
    }

    #[test]
    fn materialize_never_overwrites() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("artifact.bin");
        write_materialized_file(&destination, b"first").unwrap();
        assert!(write_materialized_file(&destination, b"second")
            .unwrap_err()
            .contains("without overwriting"));
        assert_eq!(fs::read(destination).unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_symlink_sources() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source.bin");
        let link = root.path().join("link.bin");
        fs::write(&source, b"source").unwrap();
        symlink(&source, &link).unwrap();
        assert!(read_collect_source(&link, 1024)
            .unwrap_err()
            .contains("symlink"));
    }
}
