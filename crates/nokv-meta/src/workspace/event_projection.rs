/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Internal encoding for durable workspace change events.
//!
//! Mutation modules use this helper to construct the append-only event payload.
//! Keeping it outside the read-only query module avoids coupling write paths to
//! query execution.

use super::engine::EventProjection;
use super::query_records::{ChangeEventRecord, QueryRecordError};

pub(super) fn change_event_projection(
    event: &ChangeEventRecord,
) -> Result<EventProjection, QueryRecordError> {
    Ok(EventProjection {
        payload: event.encode()?,
    })
}
