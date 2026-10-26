/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Explicit single-artifact transfer helpers for local executables.

use std::path::{Path, PathBuf};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::ffi::OsStr;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::File;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::io::{self, Read, Write};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::OwnedFd;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::MetadataExt;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::path::Component;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsFd;

#[cfg(any(target_os = "linux", target_os = "macos"))]
use rustix::fs::{
    linkat, openat, renameat_with, unlinkat, AtFlags, Mode, OFlags, RenameFlags, CWD,
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use rustix::io::Errno;

/// Path components are pinned with search-only descriptors: walking a path
/// needs execute permission on each directory, not read permission, and every
/// `*at` call below accepts such a descriptor.
#[cfg(target_os = "linux")]
const DIRECTORY_FLAGS: OFlags = OFlags::PATH
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
/// macOS `O_SEARCH` is `O_EXEC | O_DIRECTORY`; rustix has no name for it.
#[cfg(target_os = "macos")]
const DIRECTORY_FLAGS: OFlags = OFlags::from_bits_retain(0x4000_0000)
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

/// How a staged file reaches its final name without overwriting anything.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Publication {
    /// `renameat2(RENAME_NOREPLACE)`: atomic and create-only where the file
    /// system supports the flag.
    RenameNoReplace,
    /// Link the staged file under its final name, which fails with `EEXIST`
    /// when the name exists, then drop the staging name.
    Link,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Publication {
    /// The next strategy to try when a file system refuses this one, such as
    /// the Linux NFS client's `EINVAL` for every rename flag. If linking also
    /// fails, publishing must fail rather than expose a partially written file.
    fn fallback(self, error: &io::Error) -> Option<Self> {
        let code = error.raw_os_error();
        let refused = |candidates: &[Errno]| {
            candidates
                .iter()
                .any(|candidate| code == Some(candidate.raw_os_error()))
        };
        match self {
            Self::RenameNoReplace
                if refused(&[Errno::INVAL, Errno::NOSYS, Errno::NOTSUP, Errno::OPNOTSUPP]) =>
            {
                Some(Self::Link)
            }
            _ => None,
        }
    }
}

/// Publish the staged file `staging` under `name` in the pinned `directory`,
/// never replacing an existing name or exposing incomplete bytes. Returns the
/// strategy that succeeded.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish_staged(
    directory: &File,
    staging: &str,
    name: &OsStr,
    first: Publication,
) -> io::Result<Publication> {
    let mut strategy = first;
    loop {
        let attempt = match strategy {
            Publication::RenameNoReplace => renameat_with(
                directory.as_fd(),
                staging,
                directory.as_fd(),
                name,
                RenameFlags::NOREPLACE,
            )
            .map_err(io::Error::from),
            Publication::Link => linkat(
                directory.as_fd(),
                staging,
                directory.as_fd(),
                name,
                AtFlags::empty(),
            )
            .map_err(io::Error::from)
            .map(|()| {
                let _ = unlinkat(directory, staging, AtFlags::empty());
            }),
        };
        match attempt {
            Ok(()) => return Ok(strategy),
            Err(error) => match strategy.fallback(&error) {
                Some(next) => strategy = next,
                None => return Err(error),
            },
        }
    }
}

#[cfg(target_os = "macos")]
fn physical_system_alias(path: &Path) -> PathBuf {
    use std::os::unix::fs::MetadataExt;

    let mut parts = path.components();
    if parts.next() != Some(Component::RootDir) {
        return path.to_path_buf();
    }
    let Some(Component::Normal(first)) = parts.next() else {
        return path.to_path_buf();
    };
    let Some(alias) = ["var", "tmp", "etc"]
        .into_iter()
        .find(|alias| first == OsStr::new(alias))
    else {
        return path.to_path_buf();
    };
    let link = Path::new("/").join(alias);
    let expected = Path::new("private").join(alias);
    let trusted = std::fs::symlink_metadata(&link)
        .is_ok_and(|metadata| metadata.file_type().is_symlink() && metadata.uid() == 0)
        && std::fs::read_link(&link).is_ok_and(|target| target == expected);
    if !trusted {
        return path.to_path_buf();
    }
    let mut physical = PathBuf::from("/private").join(alias);
    for part in parts {
        physical.push(part.as_os_str());
    }
    physical
}

/// Pin every path component to an open directory before opening the final name.
/// A symlink in any component fails; callers can pass the physical path instead.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_parent_directory(path: &Path) -> io::Result<(OwnedFd, &OsStr)> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    #[cfg(target_os = "macos")]
    let parent = physical_system_alias(parent);
    #[cfg(target_os = "macos")]
    let parent = parent.as_path();
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let anchor = if parent.is_absolute() { "/" } else { "." };
    let mut directories = vec![openat(CWD, anchor, DIRECTORY_FLAGS, Mode::empty())?];
    for component in parent.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                let child = openat(
                    directories.last().expect("directory anchor exists"),
                    name,
                    DIRECTORY_FLAGS,
                    Mode::empty(),
                )?;
                directories.push(child);
            }
            Component::ParentDir => {
                if directories.len() > 1 {
                    directories.pop();
                } else {
                    directories[0] = openat(&directories[0], "..", DIRECTORY_FLAGS, Mode::empty())?;
                }
            }
            Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported path prefix",
                ));
            }
        }
    }
    Ok((directories.pop().expect("directory anchor exists"), name))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn read_collect_source(source: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    read_collect_source_with_hooks(source, max_bytes, || {}, || {})
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_collect_source_with_hooks(
    source: &Path,
    max_bytes: usize,
    before_open: impl FnOnce(),
    after_open: impl FnOnce(),
) -> Result<Vec<u8>, String> {
    let (directory, name) = open_parent_directory(source).map_err(|error| {
        format!(
            "cannot open collect source parent for {} without following symlinks; use its physical path: {error}",
            source.display()
        )
    })?;
    before_open();
    let descriptor = openat(
        &directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| {
        format!(
            "cannot open collect source {} without following symlinks: {error}",
            source.display()
        )
    })?;
    let mut file = File::from(descriptor);
    after_open();
    let metadata = file.metadata().map_err(|error| {
        format!(
            "cannot inspect opened collect source {}: {error}",
            source.display()
        )
    })?;
    if !metadata.is_file() {
        return Err(format!(
            "collect source {} is not a regular file",
            source.display()
        ));
    }
    let maximum = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if metadata.len() > maximum {
        return Err(format!(
            "collect source {} is {} bytes, maximum is {max_bytes}",
            source.display(),
            metadata.len()
        ));
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read collect source {}: {error}", source.display()))?;
    if bytes.len() > max_bytes {
        return Err(format!(
            "collect source {} grew to {} bytes, maximum is {max_bytes}",
            source.display(),
            bytes.len()
        ));
    }
    let after = file.metadata().map_err(|error| {
        format!(
            "cannot inspect opened collect source after reading {}: {error}",
            source.display()
        )
    })?;
    if after.len() != metadata.len() || after.modified().ok() != metadata.modified().ok() {
        return Err(format!(
            "collect source {} changed while it was being read",
            source.display()
        ));
    }
    Ok(bytes)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn write_materialized_file(destination: &Path, bytes: &[u8]) -> Result<PathBuf, String> {
    write_materialized_file_with_hook(destination, bytes, || {})
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn write_materialized_file_with_hook(
    destination: &Path,
    bytes: &[u8],
    before_create: impl FnOnce(),
) -> Result<PathBuf, String> {
    let reported = if destination.is_absolute() {
        destination.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot locate materialize destination: {error}"))?
            .join(destination)
    };
    let (directory, name) = open_parent_directory(destination).map_err(|error| {
        format!(
            "cannot open materialize parent for {} without following symlinks; use its physical path: {error}",
            destination.display()
        )
    })?;
    before_create();
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce)
        .map_err(|error| format!("cannot name a materialize staging file: {error}"))?;
    let staging_name = format!(".nokv-materialize-{:032x}.tmp", u128::from_le_bytes(nonce));
    // The process umask applies, as `File::create` would.
    let descriptor = openat(
        &directory,
        staging_name.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o666),
    )
    .map_err(|error| {
        format!(
            "cannot create staging file for materialize destination {}: {error}",
            destination.display()
        )
    })?;
    let mut output = File::from(descriptor);
    let staged = output.write_all(bytes).and_then(|()| output.sync_all());
    if let Err(error) = staged {
        let _ = unlinkat(&directory, staging_name.as_str(), AtFlags::empty());
        return Err(format!(
            "cannot write or sync staged materialize destination {}: {error}",
            destination.display()
        ));
    }
    let directory = File::from(directory);
    if let Err(error) = publish_staged(
        &directory,
        staging_name.as_str(),
        name,
        Publication::RenameNoReplace,
    ) {
        let _ = unlinkat(&directory, staging_name.as_str(), AtFlags::empty());
        return Err(format!(
            "cannot publish materialize destination {} without overwriting: {error}",
            destination.display()
        ));
    }
    let still_named = open_parent_directory(destination)
        .and_then(|(observed, _)| {
            let expected = directory.metadata()?;
            let actual = File::from(observed).metadata()?;
            Ok((expected.dev(), expected.ino()) == (actual.dev(), actual.ino()))
        })
        .unwrap_or(false);
    if !still_named {
        let _ = unlinkat(&directory, name, AtFlags::empty());
        return Err(format!(
            "materialize destination parent changed before publication completed: {}",
            destination.display()
        ));
    }
    // Materialized paths are disposable scratch. The staged file is synced,
    // while atomic publication does not promise directory durability.
    Ok(reported)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn read_collect_source(_source: &Path, _max_bytes: usize) -> Result<Vec<u8>, String> {
    Err("local collect requires Linux or macOS for no-follow source opening".to_owned())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn write_materialized_file(_destination: &Path, _bytes: &[u8]) -> Result<PathBuf, String> {
    Err("local materialize requires Linux or macOS for no-follow destination creation".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn physical_tempdir() -> (tempfile::TempDir, PathBuf) {
        let temporary = tempfile::tempdir().unwrap();
        let physical = temporary.path().canonicalize().unwrap();
        (temporary, physical)
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_reads_only_bounded_regular_files() {
        let (_temporary, root) = physical_tempdir();
        let source = root.join("result.bin");
        fs::write(&source, b"result").unwrap();
        assert_eq!(read_collect_source(&source, 6).unwrap(), b"result");
        fs::create_dir(root.join("nested")).unwrap();
        assert_eq!(
            read_collect_source(&root.join("nested/../result.bin"), 6).unwrap(),
            b"result"
        );
        assert!(read_collect_source(&source, 5)
            .unwrap_err()
            .contains("maximum"));
        assert!(read_collect_source(&root, 1024)
            .unwrap_err()
            .contains("regular file"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn materialize_never_overwrites() {
        let (_temporary, root) = physical_tempdir();
        let destination = root.join("artifact.bin");
        write_materialized_file(&destination, b"first").unwrap();
        assert!(write_materialized_file(&destination, b"second")
            .unwrap_err()
            .contains("without overwriting"));
        assert_eq!(fs::read(destination).unwrap(), b"first");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn materialize_accepts_the_root_owned_var_alias() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("output.bin");
        write_materialized_file(&target, b"complete").unwrap();
        assert_eq!(std::fs::read(target).unwrap(), b"complete");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_rejects_symlink_sources_and_parents() {
        use std::os::unix::fs::symlink;

        let (_temporary, root) = physical_tempdir();
        let parent = root.join("physical");
        fs::create_dir(&parent).unwrap();
        let source = parent.join("source.bin");
        fs::write(&source, b"source").unwrap();
        let link = parent.join("link.bin");
        symlink(&source, &link).unwrap();
        assert!(read_collect_source(&link, 1024)
            .unwrap_err()
            .contains("symlink"));
        let alias = root.join("alias");
        symlink(&parent, &alias).unwrap();
        assert!(read_collect_source(&alias.join("source.bin"), 1024)
            .unwrap_err()
            .contains("physical path"));
        assert!(
            read_collect_source(&alias.join("../physical/source.bin"), 1024)
                .unwrap_err()
                .contains("physical path")
        );
        assert!(
            write_materialized_file(&alias.join("output.bin"), b"output")
                .unwrap_err()
                .contains("physical path")
        );
        let output_link = parent.join("output.bin");
        symlink(&source, &output_link).unwrap();
        assert!(write_materialized_file(&output_link, b"output")
            .unwrap_err()
            .contains("without overwriting"));
        assert_eq!(fs::read(&source).unwrap(), b"source");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_rejects_leaf_swapped_to_symlink_before_open() {
        use std::os::unix::fs::symlink;

        let (_temporary, root) = physical_tempdir();
        let source = root.join("source.bin");
        let other = root.join("other.bin");
        fs::write(&source, b"original").unwrap();
        fs::write(&other, b"other").unwrap();
        let error = read_collect_source_with_hooks(
            &source,
            1024,
            || {
                fs::remove_file(&source).unwrap();
                symlink(&other, &source).unwrap();
            },
            || {},
        )
        .unwrap_err();
        assert!(error.contains("symlink"), "{error}");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_reads_opened_file_after_leaf_path_is_swapped() {
        use std::os::unix::fs::symlink;

        let (_temporary, root) = physical_tempdir();
        let source = root.join("source.bin");
        let other = root.join("other.bin");
        fs::write(&source, b"original").unwrap();
        fs::write(&other, b"other").unwrap();
        let bytes = read_collect_source_with_hooks(
            &source,
            1024,
            || {},
            || {
                fs::remove_file(&source).unwrap();
                symlink(&other, &source).unwrap();
            },
        )
        .unwrap();
        assert_eq!(bytes, b"original");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_parent_swap_cannot_redirect_opened_directory() {
        use std::os::unix::fs::symlink;

        let (_temporary, root) = physical_tempdir();
        let parent = root.join("source");
        let moved = root.join("source-moved");
        let other = root.join("other");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&other).unwrap();
        fs::write(parent.join("result.bin"), b"original").unwrap();
        fs::write(other.join("result.bin"), b"other").unwrap();
        let bytes = read_collect_source_with_hooks(
            &parent.join("result.bin"),
            1024,
            || {
                fs::rename(&parent, &moved).unwrap();
                symlink(&other, &parent).unwrap();
            },
            || {},
        )
        .unwrap();
        assert_eq!(bytes, b"original");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn materialize_parent_swap_cannot_redirect_creation() {
        use std::os::unix::fs::symlink;

        let (_temporary, root) = physical_tempdir();
        let parent = root.join("destination");
        let moved = root.join("destination-moved");
        let other = root.join("other");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&other).unwrap();
        let error =
            write_materialized_file_with_hook(&parent.join("artifact.bin"), b"artifact", || {
                fs::rename(&parent, &moved).unwrap();
                symlink(&other, &parent).unwrap();
            })
            .unwrap_err();
        assert!(error.contains("parent changed"));
        assert!(!moved.join("artifact.bin").exists());
        assert!(!other.join("artifact.bin").exists());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn publication_fallbacks_stay_create_only() {
        let strategy = Publication::Link;
        let (_temporary, root) = physical_tempdir();
        let handle = File::open(&root).unwrap();
        fs::write(root.join(".staging"), b"first").unwrap();
        let used = publish_staged(&handle, ".staging", OsStr::new("out.bin"), strategy).unwrap();
        assert_eq!(used, strategy);
        assert_eq!(fs::read(root.join("out.bin")).unwrap(), b"first");
        assert!(
            !root.join(".staging").exists(),
            "{strategy:?} must drop the staging name"
        );

        fs::write(root.join(".staging"), b"second").unwrap();
        let error =
            publish_staged(&handle, ".staging", OsStr::new("out.bin"), strategy).unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::AlreadyExists,
            "{strategy:?}: {error}"
        );
        assert_eq!(fs::read(root.join("out.bin")).unwrap(), b"first");
        let refused = io::Error::from_raw_os_error(Errno::INVAL.raw_os_error());
        assert_eq!(
            Publication::RenameNoReplace.fallback(&refused),
            Some(Publication::Link)
        );
        assert_eq!(Publication::Link.fallback(&refused), None);
        let exists = io::Error::from_raw_os_error(Errno::EXIST.raw_os_error());
        assert_eq!(Publication::RenameNoReplace.fallback(&exists), None);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn materialized_files_follow_the_process_umask() {
        use std::os::unix::fs::PermissionsExt;

        let (_temporary, root) = physical_tempdir();
        fs::write(root.join("control.bin"), b"control").unwrap();
        write_materialized_file(&root.join("output.bin"), b"payload").unwrap();
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode(&root.join("output.bin")),
            mode(&root.join("control.bin"))
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn search_only_ancestors_are_enough_to_collect_and_materialize() {
        use std::os::unix::fs::PermissionsExt;

        let (_temporary, root) = physical_tempdir();
        let ancestor = root.join("ancestor");
        fs::create_dir_all(ancestor.join("leaf")).unwrap();
        fs::write(ancestor.join("leaf/source.bin"), b"source").unwrap();
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o311)).unwrap();
        if fs::read_dir(&ancestor).is_ok() {
            // Running with privileges that ignore the mode: nothing to prove.
            fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        assert_eq!(
            read_collect_source(&ancestor.join("leaf/source.bin"), 1024).unwrap(),
            b"source"
        );
        write_materialized_file(&ancestor.join("leaf/output.bin"), b"output").unwrap();
        assert_eq!(
            fs::read(ancestor.join("leaf/output.bin")).unwrap(),
            b"output"
        );
        fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn collect_rejects_fifo_without_waiting_for_writer() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (_temporary, root) = physical_tempdir();
        let fifo = root.join("pipe");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            sender.send(read_collect_source(&fifo, 1024)).unwrap();
        });
        let result = receiver.recv_timeout(Duration::from_secs(2));
        if result.is_err() {
            // A read/write open releases a reader blocked in open on both
            // supported platforms; do not let a failed cleanup hide the timeout.
            let release = openat(
                CWD,
                root.join("pipe"),
                OFlags::RDWR | OFlags::NONBLOCK,
                Mode::empty(),
            );
            if release.is_ok() && receiver.recv_timeout(Duration::from_secs(2)).is_ok() {
                worker.join().unwrap();
            }
            panic!("collect blocked while opening a FIFO: {release:?}");
        }
        worker.join().unwrap();
        assert!(result.unwrap().unwrap_err().contains("regular file"));
    }
}
