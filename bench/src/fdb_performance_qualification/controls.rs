/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fs;
use std::path::Path;
use std::process::Command;

use serde::Serialize;

use crate::fdb_live_runtime::command_stdout;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ControlObservation {
    status: &'static str,
    value: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SystemControls {
    hostname: ControlObservation,
    uname: ControlObservation,
    cpu_identity: ControlObservation,
    logical_cpu_count: usize,
    process_cpu_affinity: ControlObservation,
    container_cpuset: ControlObservation,
    container_cpu_quota: ControlObservation,
    cpu_governor: ControlObservation,
    current_frequency: ControlObservation,
    thermal_observation: ControlObservation,
    clock: &'static str,
}

pub(crate) fn capture() -> SystemControls {
    SystemControls {
        hostname: command("hostname", &[]),
        uname: command("uname", &["-a"]),
        cpu_identity: cpu_identity(),
        logical_cpu_count: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        process_cpu_affinity: proc_status("Cpus_allowed_list:"),
        container_cpuset: first_file(&[
            "/sys/fs/cgroup/cpuset.cpus.effective",
            "/sys/fs/cgroup/cpuset/cpuset.cpus",
        ]),
        container_cpu_quota: first_file(&[
            "/sys/fs/cgroup/cpu.max",
            "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
        ]),
        cpu_governor: first_file(&["/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"]),
        current_frequency: first_file(&["/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq"]),
        thermal_observation: thermal(),
        clock: "std::time::Instant monotonic elapsed time",
    }
}

fn command(program: &str, arguments: &[&str]) -> ControlObservation {
    let mut command = Command::new(program);
    command.args(arguments);
    match command_stdout(&mut command) {
        Ok(value) if !value.trim().is_empty() => observed(value.trim()),
        _ => unavailable(),
    }
}

fn cpu_identity() -> ControlObservation {
    let Ok(contents) = fs::read_to_string("/proc/cpuinfo") else {
        return unavailable();
    };
    let values = contents
        .lines()
        .filter(|line| {
            line.starts_with("model name")
                || line.starts_with("Hardware")
                || line.starts_with("Processor")
        })
        .take(4)
        .collect::<Vec<_>>();
    if values.is_empty() {
        unavailable()
    } else {
        observed(values.join("; "))
    }
}

fn proc_status(field: &str) -> ControlObservation {
    let Ok(contents) = fs::read_to_string("/proc/self/status") else {
        return unavailable();
    };
    contents
        .lines()
        .find(|line| line.starts_with(field))
        .map(observed)
        .unwrap_or_else(unavailable)
}

fn first_file(paths: &[&str]) -> ControlObservation {
    paths
        .iter()
        .find_map(|path| {
            fs::read_to_string(path)
                .ok()
                .map(|value| format!("{path}: {}", value.trim()))
        })
        .filter(|value| !value.ends_with(": "))
        .map(observed)
        .unwrap_or_else(unavailable)
}

fn thermal() -> ControlObservation {
    let root = Path::new("/sys/class/thermal");
    let Ok(entries) = fs::read_dir(root) else {
        return unavailable();
    };
    let mut values = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("thermal_zone")
        })
        .filter_map(|entry| {
            fs::read_to_string(entry.path().join("temp"))
                .ok()
                .map(|value| format!("{}={}", entry.file_name().to_string_lossy(), value.trim()))
        })
        .take(64)
        .collect::<Vec<_>>();
    values.sort();
    if values.is_empty() {
        unavailable()
    } else {
        observed(values.join(","))
    }
}

fn observed(value: impl Into<String>) -> ControlObservation {
    ControlObservation {
        status: "observed",
        value: Some(value.into()),
    }
}

const fn unavailable() -> ControlObservation {
    ControlObservation {
        status: "unavailable",
        value: None,
    }
}
