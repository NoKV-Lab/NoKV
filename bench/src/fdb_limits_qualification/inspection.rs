/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;

use nokv_fdb::{
    lexicographic_successor, FdbConnectionOptions, FdbDatabase, FdbRangeRequest, FdbRuntime,
    FdbStorePrefix,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::qualification_runtime::lowercase_hex;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct StoreInspection {
    schema: &'static str,
    read_version: i64,
    row_count: usize,
    key_bytes: usize,
    value_bytes: usize,
    max_key_bytes: usize,
    max_value_bytes: usize,
    values_at_or_above_payload_size: usize,
    payload_marker_matches: usize,
    row_inventory_sha256: String,
}

pub(crate) fn inspect_store(
    cluster_file: &Path,
    prefix: &str,
    payload_bytes: usize,
    markers: &[Vec<u8>],
) -> Result<StoreInspection, String> {
    let runtime = FdbRuntime::start().map_err(|error| error.to_string())?;
    let database = FdbDatabase::open(&runtime, &FdbConnectionOptions::new(cluster_file))
        .map_err(|error| error.to_string())?;
    let transaction = database.transaction().map_err(|error| error.to_string())?;
    let read_version = transaction
        .read_version()
        .map_err(|error| error.to_string())?;
    let prefix = FdbStorePrefix::new(prefix.as_bytes()).map_err(|error| error.to_string())?;
    let mut begin = prefix.as_bytes().to_vec();
    let end = lexicographic_successor(prefix.as_bytes())
        .ok_or_else(|| "Gate 9 FDB prefix has no lexicographic successor".to_owned())?;
    let mut iteration = 1;
    let mut row_count = 0_usize;
    let mut key_bytes = 0_usize;
    let mut value_bytes = 0_usize;
    let mut max_key_bytes = 0_usize;
    let mut max_value_bytes = 0_usize;
    let mut values_at_or_above_payload_size = 0_usize;
    let mut payload_marker_matches = 0_usize;
    let mut inventory = Sha256::new();

    while begin < end {
        let page = transaction
            .get_range(&FdbRangeRequest {
                begin: begin.clone(),
                end: end.clone(),
                limit: Some(1024),
                target_bytes: 1_000_000,
                iteration,
                snapshot: true,
                reverse: false,
            })
            .map_err(|error| error.to_string())?;
        if page.items.is_empty() {
            if page.more {
                return Err(
                    "FDB returned an empty Gate 9 inspection page with more=true".to_owned(),
                );
            }
            break;
        }
        for item in &page.items {
            row_count = row_count
                .checked_add(1)
                .ok_or_else(|| "Gate 9 row count overflows usize".to_owned())?;
            key_bytes = key_bytes
                .checked_add(item.key.len())
                .ok_or_else(|| "Gate 9 key bytes overflow usize".to_owned())?;
            value_bytes = value_bytes
                .checked_add(item.value.len())
                .ok_or_else(|| "Gate 9 value bytes overflow usize".to_owned())?;
            max_key_bytes = max_key_bytes.max(item.key.len());
            max_value_bytes = max_value_bytes.max(item.value.len());
            values_at_or_above_payload_size += usize::from(item.value.len() >= payload_bytes);
            for marker in markers {
                payload_marker_matches += item
                    .value
                    .windows(marker.len())
                    .filter(|window| *window == marker.as_slice())
                    .count();
            }
            inventory.update((item.key.len() as u64).to_be_bytes());
            inventory.update(&item.key);
            inventory.update((item.value.len() as u64).to_be_bytes());
            inventory.update(&item.value);
        }
        if !page.more {
            break;
        }
        begin = page
            .items
            .last()
            .expect("nonempty page has a last row")
            .key
            .clone();
        begin.push(0);
        iteration = iteration
            .checked_add(1)
            .ok_or_else(|| "Gate 9 inspection iteration overflows usize".to_owned())?;
    }

    if row_count == 0 {
        return Err("Gate 9 FDB prefix contains no retained rows".to_owned());
    }
    if values_at_or_above_payload_size != 0 || payload_marker_matches != 0 {
        return Err("large object payload bytes appeared in retained FDB rows".to_owned());
    }

    Ok(StoreInspection {
        schema: "nokv.fdb.limits-qualification.store-inspection.v1",
        read_version,
        row_count,
        key_bytes,
        value_bytes,
        max_key_bytes,
        max_value_bytes,
        values_at_or_above_payload_size,
        payload_marker_matches,
        row_inventory_sha256: lowercase_hex(&inventory.finalize()),
    })
}
