/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::ffi::{OsStr, OsString};
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::path::Component;

#[cfg(unix)]
use rustix::fs::{
    mkdirat, openat, renameat_with, unlinkat, AtFlags, Dir, Mode, OFlags, RenameFlags,
};
#[cfg(unix)]
use rustix::io::Errno;

use nokv_protocol::RelativePath;

#[cfg(target_os = "macos")]
fn physical_system_alias(path: &Path) -> PathBuf {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalSourceFile {
    pub relative_path: RelativePath,
    pub absolute_path: PathBuf,
    #[cfg(unix)]
    identity: (u64, u64),
}

#[derive(Debug)]
pub(crate) struct LocalSourceCollection {
    pub files: Vec<LocalSourceFile>,
    #[cfg(unix)]
    anchor: File,
    #[cfg(unix)]
    components: Vec<OsString>,
    #[cfg(unix)]
    directory: File,
}

impl LocalSourceCollection {
    #[cfg(unix)]
    pub(crate) fn read_file(&self, file: &LocalSourceFile) -> Result<Vec<u8>, String> {
        self.verify_root()?;
        let components = normal_components(Path::new(file.relative_path.as_str()))?;
        let bytes = read_regular_file_from(
            &self.directory,
            &components,
            &file.absolute_path,
            Some(file.identity),
            || {},
        )?;
        self.verify_root()?;
        Ok(bytes)
    }

    #[cfg(not(unix))]
    pub(crate) fn read_file(&self, _file: &LocalSourceFile) -> Result<Vec<u8>, String> {
        Err("local collection requires Unix directory descriptors".to_owned())
    }

    #[cfg(unix)]
    fn verify_root(&self) -> Result<(), String> {
        let observed = walk_directories(&self.anchor, &self.components, false)?;
        if !same_directory(&observed, &self.directory)? {
            return Err("local collect root changed while files were being read".to_owned());
        }
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn collect_local_files(root: &Path) -> Result<LocalSourceCollection, String> {
    collect_local_files_with_hook(root, |_| {})
}

#[cfg(unix)]
fn collect_local_files_with_hook(
    root: &Path,
    mut before_entry_open: impl FnMut(&Path),
) -> Result<LocalSourceCollection, String> {
    if root.as_os_str().is_empty() {
        return Err("local collect root is empty".to_owned());
    }
    let root = if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot inspect current directory: {error}"))?
            .join(root)
    };
    #[cfg(target_os = "macos")]
    let root = physical_system_alias(&root);
    let anchor = File::open("/")
        .map_err(|error| format!("cannot open local collect path anchor: {error}"))?;
    let components = normal_components(&root)?;
    let directory = walk_directories(&anchor, &components, false)?;
    before_entry_open(&root);

    let mut files = Vec::new();
    visit_directory(
        &root,
        &directory,
        Path::new(""),
        &mut files,
        &mut before_entry_open,
    )?;
    files.sort_by(|left, right| {
        left.relative_path
            .as_str()
            .cmp(right.relative_path.as_str())
    });
    Ok(LocalSourceCollection {
        files,
        anchor,
        components,
        directory,
    })
}

#[cfg(not(unix))]
pub(crate) fn collect_local_files(_root: &Path) -> Result<LocalSourceCollection, String> {
    Err("local collection requires Unix directory descriptors".to_owned())
}

#[cfg(unix)]
fn visit_directory(
    root: &Path,
    directory: &File,
    relative_directory: &Path,
    files: &mut Vec<LocalSourceFile>,
    before_entry_open: &mut impl FnMut(&Path),
) -> Result<(), String> {
    let entries = Dir::read_from(directory).map_err(|error| {
        format!(
            "cannot list local directory {}: {error}",
            root.join(relative_directory).display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "cannot read an entry below {}: {error}",
                root.join(relative_directory).display()
            )
        })?;
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        let relative = relative_directory.join(OsStr::from_bytes(name));
        let path = root.join(&relative);
        before_entry_open(&path);
        let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        let descriptor =
            openat(directory, OsStr::from_bytes(name), flags, Mode::empty()).map_err(|error| {
                format!(
                    "cannot open local collect path {} without following symlinks: {error}",
                    path.display()
                )
            })?;
        let opened = File::from(descriptor);
        let metadata = opened
            .metadata()
            .map_err(|error| format!("cannot inspect local path {}: {error}", path.display()))?;
        if metadata.is_dir() {
            visit_directory(root, &opened, &relative, files, before_entry_open)?;
            continue;
        }
        if !metadata.is_file() {
            return Err(format!(
                "local collect path {} is not a regular file",
                path.display()
            ));
        }
        let relative = portable_relative_path(&relative)?;
        files.push(LocalSourceFile {
            relative_path: relative,
            absolute_path: path,
            identity: (metadata.dev(), metadata.ino()),
        });
    }
    Ok(())
}

#[cfg(unix)]
fn portable_relative_path(path: &Path) -> Result<RelativePath, String> {
    let mut components = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(format!(
                "local relative path {} contains a non-normal component",
                path.display()
            ));
        };
        let component = component
            .to_str()
            .ok_or_else(|| format!("local relative path {} is not valid UTF-8", path.display()))?;
        components.push(component);
    }
    RelativePath::new(components.join("/"))
        .map_err(|error| format!("local relative path {} is invalid: {error}", path.display()))
}

#[cfg(unix)]
pub(crate) fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    read_regular_file_with_hook(path, || {})
}

#[cfg(not(unix))]
pub(crate) fn read_regular_file(_path: &Path) -> Result<Vec<u8>, String> {
    Err("local file reads require Unix directory descriptors".to_owned())
}

#[cfg(unix)]
fn read_regular_file_with_hook(path: &Path, before_open: impl FnOnce()) -> Result<Vec<u8>, String> {
    if path.as_os_str().is_empty() {
        return Err("local file path is empty".to_owned());
    }
    let anchor = File::open(if path.is_absolute() { "/" } else { "." })
        .map_err(|error| format!("cannot open local file path anchor: {error}"))?;
    #[cfg(target_os = "macos")]
    let physical = physical_system_alias(path);
    #[cfg(target_os = "macos")]
    let source = physical.as_path();
    #[cfg(not(target_os = "macos"))]
    let source = path;
    let components = normal_components(source)?;
    read_regular_file_from(&anchor, &components, path, None, before_open)
}

#[cfg(unix)]
fn read_regular_file_from(
    anchor: &File,
    components: &[OsString],
    path: &Path,
    expected_identity: Option<(u64, u64)>,
    before_open: impl FnOnce(),
) -> Result<Vec<u8>, String> {
    let (file_name, directories) = components
        .split_last()
        .ok_or_else(|| format!("local file path {} is not a regular file", path.display()))?;
    let parent = walk_directories(anchor, directories, false)?;
    before_open();

    // NONBLOCK prevents a swapped-in FIFO from waiting for a writer before
    // the descriptor can be checked. The file type and bytes use the same fd.
    let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let descriptor = openat(&parent, file_name, flags, Mode::empty()).map_err(|error| {
        format!(
            "cannot open local file {} without following symlinks: {error}",
            path.display()
        )
    })?;
    let mut file = File::from(descriptor);
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot inspect local file {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "local file path {} is not a regular file",
            path.display()
        ));
    }
    if expected_identity.is_some_and(|identity| identity != (metadata.dev(), metadata.ino())) {
        return Err(format!(
            "local file path {} changed since enumeration",
            path.display()
        ));
    }
    verify_read_parent(anchor, directories, &parent)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read local file {}: {error}", path.display()))?;
    let after = file.metadata().map_err(|error| {
        format!(
            "cannot inspect opened local file {}: {error}",
            path.display()
        )
    })?;
    if after.len() != metadata.len() || after.modified().ok() != metadata.modified().ok() {
        return Err(format!(
            "local file path {} changed while it was being read",
            path.display()
        ));
    }
    verify_read_parent(anchor, directories, &parent)?;
    Ok(bytes)
}

#[cfg(unix)]
fn normal_components(path: &Path) -> Result<Vec<OsString>, String> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => components.push(name.to_owned()),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(format!(
                    "local path {} has a non-normal component",
                    path.display()
                ));
            }
        }
    }
    Ok(components)
}

#[cfg(unix)]
fn verify_read_parent(
    anchor: &File,
    directories: &[OsString],
    expected: &File,
) -> Result<(), String> {
    let observed = walk_directories(anchor, directories, false)?;
    if !same_directory(&observed, expected)? {
        return Err("local file parent changed while it was being read".to_owned());
    }
    Ok(())
}

pub(crate) fn join_remote_path(
    prefix: Option<&RelativePath>,
    relative: &RelativePath,
) -> Result<RelativePath, String> {
    let joined = match prefix {
        Some(prefix) => format!("{}/{}", prefix.as_str(), relative.as_str()),
        None => relative.as_str().to_owned(),
    };
    RelativePath::new(joined).map_err(|error| format!("collected remote path is invalid: {error}"))
}

pub(crate) fn materialized_relative_path(
    remote: &RelativePath,
    prefix: Option<&RelativePath>,
) -> Result<RelativePath, String> {
    let remote = remote.as_str();
    let relative = match prefix {
        None => remote,
        Some(prefix) if remote == prefix.as_str() => remote
            .rsplit('/')
            .next()
            .expect("validated relative paths have one component"),
        Some(prefix) => remote
            .strip_prefix(prefix.as_str())
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| {
                format!(
                    "server returned path {remote:?} outside requested component prefix {:?}",
                    prefix.as_str()
                )
            })?,
    };
    RelativePath::new(relative.to_owned())
        .map_err(|error| format!("materialized relative path is invalid: {error}"))
}

#[derive(Debug)]
pub(crate) struct MaterializeRoot {
    #[cfg(unix)]
    anchor: File,
    #[cfg(unix)]
    components: Vec<OsString>,
    #[cfg(unix)]
    directory: File,
    path: PathBuf,
}

#[derive(Debug)]
pub(crate) struct CreatedMaterializedFile {
    pub(crate) path: PathBuf,
    #[cfg(unix)]
    parent: File,
    #[cfg(unix)]
    file_name: OsString,
    #[cfg(unix)]
    identity: (u64, u64),
}

impl CreatedMaterializedFile {
    pub(crate) fn remove(self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let descriptor = match openat(
                &self.parent,
                &self.file_name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(descriptor) => descriptor,
                Err(Errno::NOENT) => return Ok(()),
                Err(error) => return Err(io::Error::from(error)),
            };
            let metadata = File::from(descriptor).metadata()?;
            if (metadata.dev(), metadata.ino()) != self.identity {
                return Err(io::Error::other(
                    "materialized target changed before rollback",
                ));
            }
            unlinkat(&self.parent, &self.file_name, AtFlags::empty()).map_err(io::Error::from)
        }
        #[cfg(not(unix))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "local materialization requires Unix directory descriptors",
            ))
        }
    }
}

#[cfg(unix)]
fn open_directory(parent: &File, name: &OsStr, create: bool) -> Result<File, String> {
    let flags = OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let opened = openat(parent, name, flags, Mode::empty());
    let descriptor = match opened {
        Ok(descriptor) => descriptor,
        Err(Errno::NOENT) if create => {
            match mkdirat(parent, name, Mode::from_bits_retain(0o700)) {
                Ok(()) | Err(Errno::EXIST) => {}
                Err(error) => {
                    return Err(format!(
                        "cannot create materialize directory {:?}: {error}",
                        name
                    ));
                }
            }
            openat(parent, name, flags, Mode::empty()).map_err(|error| {
                format!(
                    "cannot open materialize directory {:?} without following symlinks: {error}",
                    name
                )
            })?
        }
        Err(error) => {
            return Err(format!(
                "cannot open materialize directory {:?} without following symlinks: {error}",
                name
            ));
        }
    };
    Ok(File::from(descriptor))
}

#[cfg(unix)]
fn walk_directories(anchor: &File, components: &[OsString], create: bool) -> Result<File, String> {
    let mut directory = anchor
        .try_clone()
        .map_err(|error| format!("cannot duplicate materialize directory descriptor: {error}"))?;
    for component in components {
        directory = open_directory(&directory, component, create)?;
    }
    Ok(directory)
}

#[cfg(unix)]
fn same_directory(left: &File, right: &File) -> Result<bool, String> {
    let left = left
        .metadata()
        .map_err(|error| format!("cannot inspect materialize directory: {error}"))?;
    let right = right
        .metadata()
        .map_err(|error| format!("cannot inspect materialize directory: {error}"))?;
    Ok((left.dev(), left.ino()) == (right.dev(), right.ino()))
}

#[cfg(unix)]
impl MaterializeRoot {
    fn verify_directory(&self, directories: &[&str], expected: &File) -> Result<(), String> {
        let observed = walk_directories(&self.anchor, &self.components, false)?;
        if !same_directory(&observed, &self.directory)? {
            return Err("materialize root changed while files were being created".to_owned());
        }
        let mut observed_parent = observed;
        for directory in directories {
            observed_parent = open_directory(&observed_parent, OsStr::new(directory), false)?;
        }
        if !same_directory(&observed_parent, expected)? {
            return Err("materialize parent changed while files were being created".to_owned());
        }
        Ok(())
    }
}

#[cfg(unix)]
pub(crate) fn prepare_materialize_root(root: &Path) -> Result<MaterializeRoot, String> {
    if root.as_os_str().is_empty() {
        return Err("local materialize root is empty".to_owned());
    }
    #[cfg(target_os = "macos")]
    let physical = physical_system_alias(root);
    #[cfg(target_os = "macos")]
    let root = physical.as_path();
    let anchor = File::open(if root.is_absolute() { "/" } else { "." })
        .map_err(|error| format!("cannot open materialize path anchor: {error}"))?;
    let path = if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot inspect current directory: {error}"))?
            .join(root)
    };
    let mut components = Vec::new();
    for component in root.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => components.push(name.to_owned()),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(format!(
                    "local materialize root {} has a non-normal component",
                    root.display()
                ));
            }
        }
    }
    let directory = walk_directories(&anchor, &components, true)?;
    Ok(MaterializeRoot {
        anchor,
        components,
        directory,
        path,
    })
}

#[cfg(not(unix))]
pub(crate) fn prepare_materialize_root(_root: &Path) -> Result<MaterializeRoot, String> {
    Err("local materialization requires Unix directory descriptors".to_owned())
}

pub(crate) fn create_materialized_file(
    root: &MaterializeRoot,
    relative: &RelativePath,
    bytes: &[u8],
) -> Result<CreatedMaterializedFile, String> {
    #[cfg(unix)]
    {
        create_materialized_file_with_hook(root, relative, bytes, || {})
    }
    #[cfg(not(unix))]
    {
        let _ = (root, relative, bytes);
        Err("local materialization requires Unix directory descriptors".to_owned())
    }
}

#[cfg(unix)]
fn create_materialized_file_with_hook(
    root: &MaterializeRoot,
    relative: &RelativePath,
    bytes: &[u8],
    before_open: impl FnOnce(),
) -> Result<CreatedMaterializedFile, String> {
    root.verify_directory(&[], &root.directory)?;
    let components = relative.as_str().split('/').collect::<Vec<_>>();
    let (file_name, directories) = components
        .split_last()
        .expect("validated relative paths have one component");
    let mut parent = root
        .directory
        .try_clone()
        .map_err(|error| format!("cannot duplicate materialize root descriptor: {error}"))?;
    for directory in directories {
        parent = open_directory(&parent, OsStr::new(directory), true)?;
    }
    root.verify_directory(directories, &parent)?;
    before_open();

    let target = root.path.join(relative.as_str());
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce)
        .map_err(|error| format!("cannot name materialize staging file: {error}"))?;
    let staging_name = format!(".nokv-materialize-{:032x}.tmp", u128::from_le_bytes(nonce));
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let descriptor = openat(
        &parent,
        staging_name.as_str(),
        flags,
        Mode::from_bits_retain(0o600),
    )
    .map_err(|error| {
        format!(
            "cannot create staging file for materialized file {}: {error}",
            target.display()
        )
    })?;
    let mut file = File::from(descriptor);
    let write_result = file.write_all(bytes).and_then(|()| file.sync_all());
    if let Err(error) = write_result {
        let _ = unlinkat(&parent, staging_name.as_str(), AtFlags::empty());
        return Err(format!(
            "cannot write or sync staged materialized file {}: {error}",
            target.display()
        ));
    }
    if let Err(error) = root.verify_directory(directories, &parent) {
        let _ = unlinkat(&parent, staging_name.as_str(), AtFlags::empty());
        return Err(error);
    }
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(error) => {
            let _ = unlinkat(&parent, staging_name.as_str(), AtFlags::empty());
            return Err(format!("cannot inspect staged materialized file: {error}"));
        }
    };
    if let Err(error) = renameat_with(
        &parent,
        staging_name.as_str(),
        &parent,
        *file_name,
        RenameFlags::NOREPLACE,
    ) {
        let _ = unlinkat(&parent, staging_name.as_str(), AtFlags::empty());
        return Err(format!(
            "cannot publish materialized file {} without overwriting: {error}",
            target.display()
        ));
    }
    let created = CreatedMaterializedFile {
        path: target,
        parent,
        file_name: OsString::from(*file_name),
        identity: (metadata.dev(), metadata.ino()),
    };
    if let Err(error) = root.verify_directory(directories, &created.parent) {
        let _ = created.remove();
        return Err(error);
    }
    created.parent.sync_all().map_err(|error| {
        format!(
            "materialized file {} was published but its directory sync failed: {error}",
            created.path.display()
        )
    })?;
    Ok(created)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;

    use super::*;

    #[test]
    fn component_prefix_never_matches_a_sibling_name() {
        let prefix = RelativePath::new("runs/a").unwrap();
        let child = RelativePath::new("runs/a/output.bin").unwrap();
        let sibling = RelativePath::new("runs/ab/output.bin").unwrap();
        assert_eq!(
            materialized_relative_path(&child, Some(&prefix))
                .unwrap()
                .as_str(),
            "output.bin"
        );
        assert!(materialized_relative_path(&sibling, Some(&prefix)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn collect_is_sorted_and_uses_normalized_relative_paths() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        fs::create_dir(base.join("nested")).unwrap();
        fs::write(base.join("z.bin"), b"z").unwrap();
        fs::write(base.join("nested/a.bin"), b"a").unwrap();

        let files = collect_local_files(&base).unwrap();
        assert_eq!(
            files
                .files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["nested/a.bin", "z.bin"]
        );
        assert_eq!(files.read_file(&files.files[0]).unwrap(), b"a");
        assert_eq!(files.read_file(&files.files[1]).unwrap(), b"z");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn local_adapters_accept_the_root_owned_var_alias() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("source.bin"), b"source").unwrap();
        let collected = collect_local_files(&root).unwrap();
        assert_eq!(collected.read_file(&collected.files[0]).unwrap(), b"source");
        let materialize = prepare_materialize_root(&directory.path().join("out")).unwrap();
        let relative = RelativePath::new("target.bin").unwrap();
        create_materialized_file(&materialize, &relative, b"complete").unwrap();
        assert_eq!(
            fs::read(directory.path().join("out/target.bin")).unwrap(),
            b"complete"
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialize_never_overwrites_an_existing_target() {
        let root = tempfile::tempdir().unwrap();
        let root = prepare_materialize_root(&root.path().canonicalize().unwrap()).unwrap();
        let relative = RelativePath::new("output.bin").unwrap();
        create_materialized_file(&root, &relative, b"first").unwrap();
        assert!(create_materialized_file(&root, &relative, b"second").is_err());
        assert_eq!(fs::read(root.path.join("output.bin")).unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn materialize_rejects_root_and_child_symlink_components() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, base.join("root-link")).unwrap();
        assert!(prepare_materialize_root(&base.join("root-link"))
            .unwrap_err()
            .contains("symlink"));

        let root = prepare_materialize_root(&base.join("root")).unwrap();
        symlink(&outside, root.path.join("child")).unwrap();
        let relative = RelativePath::new("child/output.bin").unwrap();
        assert!(create_materialized_file(&root, &relative, b"payload")
            .unwrap_err()
            .contains("symlink"));
        assert!(!outside.join("output.bin").exists());
    }

    #[cfg(unix)]
    #[test]
    fn root_swap_between_validation_and_create_cannot_redirect_write() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root_path = base.join("root");
        let held = base.join("held");
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        let root = prepare_materialize_root(&root_path).unwrap();
        let relative = RelativePath::new("output.bin").unwrap();
        let error = create_materialized_file_with_hook(&root, &relative, b"payload", || {
            fs::rename(&root_path, &held).unwrap();
            symlink(&outside, &root_path).unwrap();
        })
        .unwrap_err();
        assert!(error.contains("symlink") || error.contains("changed"));
        assert!(!outside.join("output.bin").exists());
        assert!(!held.join("output.bin").exists());
    }

    #[cfg(unix)]
    #[test]
    fn child_swap_between_validation_and_create_cannot_redirect_write() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = prepare_materialize_root(&base.join("root")).unwrap();
        fs::create_dir(root.path.join("child")).unwrap();
        let held = root.path.join("held");
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        let relative = RelativePath::new("child/output.bin").unwrap();
        let error = create_materialized_file_with_hook(&root, &relative, b"payload", || {
            fs::rename(root.path.join("child"), &held).unwrap();
            symlink(&outside, root.path.join("child")).unwrap();
        })
        .unwrap_err();
        assert!(error.contains("symlink") || error.contains("changed"));
        assert!(!outside.join("output.bin").exists());
        assert!(!held.join("output.bin").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rollback_removes_from_the_pinned_directory_after_root_swap() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root_path = base.join("root");
        let held = base.join("held");
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("output.bin"), b"keep").unwrap();
        let root = prepare_materialize_root(&root_path).unwrap();
        let relative = RelativePath::new("output.bin").unwrap();
        let created = create_materialized_file(&root, &relative, b"payload").unwrap();
        fs::rename(&root_path, &held).unwrap();
        symlink(&outside, &root_path).unwrap();
        created.remove().unwrap();
        assert!(!held.join("output.bin").exists());
        assert_eq!(fs::read(outside.join("output.bin")).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_symlinks_instead_of_following_them() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), base.join("escape.bin")).unwrap();
        assert!(collect_local_files(&base).unwrap_err().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_symlink_in_source_root() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let source = base.join("source");
        let link = base.join("link");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("file.bin"), b"bytes").unwrap();
        symlink(&source, &link).unwrap();
        assert!(collect_local_files(&link).unwrap_err().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn collect_root_swap_before_enumeration_stays_on_pinned_directory() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = base.join("root");
        let held = base.join("held");
        let outside = base.join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(root.join("safe.bin"), b"safe").unwrap();
        fs::write(outside.join("secret.bin"), b"outside").unwrap();

        let files = collect_local_files_with_hook(&root, |path| {
            if path == root {
                fs::rename(&root, &held).unwrap();
                symlink(&outside, &root).unwrap();
            }
        })
        .unwrap();
        assert_eq!(files.files.len(), 1);
        assert_eq!(files.files[0].relative_path.as_str(), "safe.bin");
        assert!(files
            .read_file(&files.files[0])
            .unwrap_err()
            .contains("symlink"));
        assert_eq!(fs::read(outside.join("secret.bin")).unwrap(), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_replacement_root_directory_before_reading() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = base.join("root");
        let held = base.join("held");
        let outside = base.join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(root.join("file.bin"), b"original").unwrap();
        fs::write(outside.join("file.bin"), b"outside").unwrap();

        let files = collect_local_files(&root).unwrap();
        fs::rename(&root, &held).unwrap();
        fs::rename(&outside, &root).unwrap();
        assert!(files
            .read_file(&files.files[0])
            .unwrap_err()
            .contains("changed"));
        assert_eq!(fs::read(root.join("file.bin")).unwrap(), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_replacement_regular_file_after_enumeration() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = base.join("root");
        fs::create_dir(&root).unwrap();
        let path = root.join("file.bin");
        fs::write(&path, b"original").unwrap();

        let files = collect_local_files(&root).unwrap();
        fs::rename(&path, root.join("held.bin")).unwrap();
        fs::write(&path, b"replacement").unwrap();
        assert!(files
            .read_file(&files.files[0])
            .unwrap_err()
            .contains("changed since enumeration"));
    }

    #[cfg(unix)]
    #[test]
    fn collect_child_swap_during_enumeration_rejects_symlink() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = base.join("root");
        let child = root.join("child");
        let held = root.join("held");
        let outside = base.join("outside");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&child).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(child.join("safe.bin"), b"safe").unwrap();
        fs::write(outside.join("secret.bin"), b"outside").unwrap();

        let error = collect_local_files_with_hook(&root, |path| {
            if path == child {
                fs::rename(&child, &held).unwrap();
                symlink(&outside, &child).unwrap();
            }
        })
        .unwrap_err();
        assert!(error.contains("symlink"));
        assert!(!error.contains("secret.bin"));
        assert_eq!(fs::read(outside.join("secret.bin")).unwrap(), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn read_regular_file_rejects_leaf_swap_after_parent_open() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let source = base.join("source.bin");
        let held = base.join("held.bin");
        let outside = base.join("outside.bin");
        fs::write(&source, b"original").unwrap();
        fs::write(&outside, b"outside").unwrap();

        let error = read_regular_file_with_hook(&source, || {
            fs::rename(&source, &held).unwrap();
            symlink(&outside, &source).unwrap();
        })
        .unwrap_err();
        assert!(error.contains("symlink"));
        assert_eq!(fs::read(&outside).unwrap(), b"outside");
    }

    #[cfg(unix)]
    #[test]
    fn read_regular_file_rejects_parent_swap_after_parent_open() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let child = base.join("child");
        let held = base.join("held");
        let outside = base.join("outside");
        fs::create_dir(&child).unwrap();
        fs::create_dir(&outside).unwrap();
        fs::write(child.join("source.bin"), b"original").unwrap();
        fs::write(outside.join("source.bin"), b"outside").unwrap();

        let error = read_regular_file_with_hook(&child.join("source.bin"), || {
            fs::rename(&child, &held).unwrap();
            symlink(&outside, &child).unwrap();
        })
        .unwrap_err();
        assert!(error.contains("symlink") || error.contains("changed"));
        assert_eq!(fs::read(held.join("source.bin")).unwrap(), b"original");
        assert_eq!(fs::read(outside.join("source.bin")).unwrap(), b"outside");
    }
}
