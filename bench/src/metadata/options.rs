/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::PathBuf;

use nokv_protocol::PageRequest;

use super::fixture::fixture_path_count;

const DEFAULT_ITERATIONS: u64 = 1_000;
const DEFAULT_WARMUP: u64 = 100;
const DEFAULT_DIRECT_CHILDREN: usize = 96;
const DEFAULT_LEAVES_PER_CHILD: usize = 64;
const DEFAULT_PAGE_LIMIT: u32 = 32;
const DEFAULT_SEED: u64 = 42;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataOptions {
    pub iterations: u64,
    pub warmup: u64,
    pub direct_children: usize,
    pub leaves_per_child: usize,
    pub page_limit: u32,
    pub seed: u64,
    pub metadata_dir: Option<PathBuf>,
    pub revision: String,
    pub harness_revision: String,
    pub dirty_worktree: bool,
}

impl Default for MetadataOptions {
    fn default() -> Self {
        Self {
            iterations: DEFAULT_ITERATIONS,
            warmup: DEFAULT_WARMUP,
            direct_children: DEFAULT_DIRECT_CHILDREN,
            leaves_per_child: DEFAULT_LEAVES_PER_CHILD,
            page_limit: DEFAULT_PAGE_LIMIT,
            seed: DEFAULT_SEED,
            metadata_dir: None,
            revision: String::new(),
            harness_revision: String::new(),
            dirty_worktree: false,
        }
    }
}

impl MetadataOptions {
    pub fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut options = Self::default();
        let mut arguments = arguments.peekable();
        while let Some(flag) = arguments.next() {
            match flag.as_str() {
                "--iterations" => {
                    options.iterations = positive_u64(&flag, next_value(&mut arguments, &flag)?)?;
                }
                "--warmup" => {
                    options.warmup = parsed_u64(&flag, next_value(&mut arguments, &flag)?)?;
                }
                "--direct-children" => {
                    options.direct_children =
                        positive_usize(&flag, next_value(&mut arguments, &flag)?)?;
                }
                "--leaves-per-child" => {
                    options.leaves_per_child =
                        positive_usize(&flag, next_value(&mut arguments, &flag)?)?;
                }
                "--page-limit" => {
                    let value = positive_u64(&flag, next_value(&mut arguments, &flag)?)?;
                    options.page_limit = u32::try_from(value)
                        .map_err(|_| "--page-limit does not fit u32".to_owned())?;
                }
                "--seed" => {
                    options.seed = parsed_u64(&flag, next_value(&mut arguments, &flag)?)?;
                }
                "--metadata-dir" => {
                    options.metadata_dir = Some(PathBuf::from(next_value(&mut arguments, &flag)?));
                }
                "--revision" => {
                    options.revision = next_value(&mut arguments, &flag)?;
                }
                "--harness-revision" => {
                    options.harness_revision = next_value(&mut arguments, &flag)?;
                }
                "--dirty-worktree" => options.dirty_worktree = true,
                _ => return Err(format!("unknown metadata option {flag:?}\n{}", usage())),
            }
        }
        options.validate()?;
        Ok(options)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        if self.iterations == 0 {
            return Err("--iterations must be greater than zero".to_owned());
        }
        if self.page_limit == 0 || self.page_limit > PageRequest::MAX_LIMIT {
            return Err(format!(
                "--page-limit must be between 1 and {}",
                PageRequest::MAX_LIMIT
            ));
        }
        let minimum_children = usize::try_from(self.page_limit)
            .expect("u32 always fits usize")
            .saturating_mul(2);
        if self.direct_children < minimum_children {
            return Err(format!(
                "--direct-children must be at least twice --page-limit ({minimum_children})"
            ));
        }
        if self.revision.trim().is_empty() {
            return Err("--revision must not be empty or omitted".to_owned());
        }
        if self.harness_revision.trim().is_empty() {
            return Err("--harness-revision must not be empty or omitted".to_owned());
        }
        fixture_path_count(self)?;
        Ok(())
    }
}

pub fn usage() -> &'static str {
    "usage: nokv-bench metadata [--iterations N] [--warmup N] \
     [--direct-children N] [--leaves-per-child N] [--page-limit N] \
     [--seed N] [--metadata-dir PATH] --revision LABEL \
     --harness-revision DIGEST [--dirty-worktree]"
}

fn next_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parsed_u64(flag: &str, value: String) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be an unsigned integer"))
}

pub(super) fn cache_state(warmup: u64) -> &'static str {
    if warmup == 0 {
        "uncontrolled"
    } else {
        "same_request_warmup"
    }
}

fn positive_u64(flag: &str, value: String) -> Result<u64, String> {
    let value = parsed_u64(flag, value)?;
    if value == 0 {
        return Err(format!("{flag} must be greater than zero"));
    }
    Ok(value)
}

fn positive_usize(flag: &str, value: String) -> Result<usize, String> {
    let value = positive_u64(flag, value)?;
    usize::try_from(value).map_err(|_| format!("{flag} does not fit usize"))
}
