/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use nokv_protocol::RelativePath;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalSourceFile {
    pub relative_path: RelativePath,
    pub absolute_path: PathBuf,
}

pub(crate) fn collect_local_files(root: &Path) -> Result<Vec<LocalSourceFile>, String> {
    let root = root.canonicalize().map_err(|error| {
        format!(
            "cannot canonicalize local directory {}: {error}",
            root.display()
        )
    })?;
    let metadata = fs::metadata(&root)
        .map_err(|error| format!("cannot inspect local directory {}: {error}", root.display()))?;
    if !metadata.is_dir() {
        return Err(format!(
            "local collect root {} is not a directory",
            root.display()
        ));
    }

    let mut files = Vec::new();
    visit_directory(&root, &root, &mut files)?;
    files.sort_by(|left, right| {
        left.relative_path
            .as_str()
            .cmp(right.relative_path.as_str())
    });
    Ok(files)
}

fn visit_directory(
    root: &Path,
    directory: &Path,
    files: &mut Vec<LocalSourceFile>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory).map_err(|error| {
        format!(
            "cannot list local directory {}: {error}",
            directory.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "cannot read an entry below {}: {error}",
                directory.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect local path {}: {error}", path.display()))?;
        if file_type.is_symlink() {
            return Err(format!(
                "local collect path {} is a symlink; symlinks are not followed",
                path.display()
            ));
        }
        if file_type.is_dir() {
            visit_directory(root, &path, files)?;
            continue;
        }
        if !file_type.is_file() {
            return Err(format!(
                "local collect path {} is not a regular file",
                path.display()
            ));
        }
        let canonical = path.canonicalize().map_err(|error| {
            format!("cannot canonicalize local file {}: {error}", path.display())
        })?;
        if !canonical.starts_with(root) {
            return Err(format!(
                "local collect path {} escapes root {}",
                canonical.display(),
                root.display()
            ));
        }
        let relative = canonical.strip_prefix(root).map_err(|error| {
            format!(
                "cannot derive local path below {} from {}: {error}",
                root.display(),
                canonical.display()
            )
        })?;
        let relative = portable_relative_path(relative)?;
        files.push(LocalSourceFile {
            relative_path: relative,
            absolute_path: canonical,
        });
    }
    Ok(())
}

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

pub(crate) fn prepare_materialize_root(root: &Path) -> Result<PathBuf, String> {
    fs::create_dir_all(root).map_err(|error| {
        format!(
            "cannot create local materialize directory {}: {error}",
            root.display()
        )
    })?;
    let root = root.canonicalize().map_err(|error| {
        format!(
            "cannot canonicalize local materialize directory {}: {error}",
            root.display()
        )
    })?;
    if !root.is_dir() {
        return Err(format!(
            "local materialize root {} is not a directory",
            root.display()
        ));
    }
    Ok(root)
}

pub(crate) fn create_materialized_file(
    root: &Path,
    relative: &RelativePath,
    bytes: &[u8],
) -> Result<PathBuf, String> {
    let components = relative.as_str().split('/').collect::<Vec<_>>();
    let (file_name, directories) = components
        .split_last()
        .expect("validated relative paths have one component");
    let mut parent = root.to_path_buf();
    for directory in directories {
        parent.push(directory);
        match fs::symlink_metadata(&parent) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "materialize directory {} is a symlink",
                    parent.display()
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "materialize parent {} is not a directory",
                    parent.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&parent).map_err(|error| {
                    format!(
                        "cannot create materialize directory {}: {error}",
                        parent.display()
                    )
                })?;
            }
            Err(error) => {
                return Err(format!(
                    "cannot inspect materialize directory {}: {error}",
                    parent.display()
                ));
            }
        }
        let canonical_parent = parent.canonicalize().map_err(|error| {
            format!(
                "cannot canonicalize materialize directory {}: {error}",
                parent.display()
            )
        })?;
        if !canonical_parent.starts_with(root) {
            return Err(format!(
                "materialize directory {} escapes root {}",
                canonical_parent.display(),
                root.display()
            ));
        }
        parent = canonical_parent;
    }

    let target = parent.join(file_name);
    let mut file = open_new_file(&target)?;
    file.write_all(bytes).map_err(|error| {
        format!(
            "cannot write materialized file {}: {error}",
            target.display()
        )
    })?;
    file.sync_all().map_err(|error| {
        format!(
            "cannot sync materialized file {}: {error}",
            target.display()
        )
    })?;
    Ok(target)
}

fn open_new_file(target: &Path) -> Result<File, String> {
    if let Ok(metadata) = fs::symlink_metadata(target) {
        let kind = if metadata.file_type().is_symlink() {
            "symlink"
        } else if metadata.is_dir() {
            "directory"
        } else {
            "existing file"
        };
        return Err(format!(
            "materialize target {} already exists as {kind}",
            target.display()
        ));
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .map_err(|error| {
            format!(
                "cannot create materialized file {}: {error}",
                target.display()
            )
        })
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn collect_is_sorted_and_uses_normalized_relative_paths() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("nested")).unwrap();
        fs::write(root.path().join("z.bin"), b"z").unwrap();
        fs::write(root.path().join("nested/a.bin"), b"a").unwrap();

        let files = collect_local_files(root.path()).unwrap();
        assert_eq!(
            files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["nested/a.bin", "z.bin"]
        );
    }

    #[test]
    fn materialize_never_overwrites_an_existing_target() {
        let root = tempfile::tempdir().unwrap();
        let root = prepare_materialize_root(root.path()).unwrap();
        let relative = RelativePath::new("output.bin").unwrap();
        create_materialized_file(&root, &relative, b"first").unwrap();
        assert!(create_materialized_file(&root, &relative, b"second").is_err());
        assert_eq!(fs::read(root.join("output.bin")).unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn collect_rejects_symlinks_instead_of_following_them() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), root.path().join("escape.bin")).unwrap();
        assert!(collect_local_files(root.path())
            .unwrap_err()
            .contains("symlink"));
    }
}
