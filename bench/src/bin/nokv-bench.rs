/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::env;

use nokv_bench::metadata::{self, MetadataOptions};

fn main() {
    if let Err(message) = run(env::args().skip(1)) {
        eprintln!("error: {message}");
        std::process::exit(2);
    }
}

fn run(mut arguments: impl Iterator<Item = String>) -> Result<(), String> {
    match arguments.next().as_deref() {
        Some("metadata") => {
            let report = metadata::run(MetadataOptions::parse(arguments)?)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(|error| error.to_string())?
            );
            Ok(())
        }
        _ => Err(metadata::usage().to_owned()),
    }
}
