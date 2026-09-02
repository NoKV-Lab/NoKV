/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_bench::fdb_performance_qualification::{run, QualificationOptions, LIVE_GATE_ENV};

fn main() {
    if let Err(error) = main_result() {
        eprintln!("nokv-fdb performance qualification failed: {error}");
        std::process::exit(1);
    }
}

fn main_result() -> Result<(), String> {
    if std::env::var(LIVE_GATE_ENV).as_deref() != Ok("1") {
        return Err(format!(
            "live qualification is disabled; set {LIVE_GATE_ENV}=1 explicitly"
        ));
    }
    let options = QualificationOptions::parse(std::env::args().skip(1))?;
    let evidence = run(options)?;
    println!("{}", evidence.display());
    Ok(())
}
