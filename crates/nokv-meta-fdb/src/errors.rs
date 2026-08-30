/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta_store::LimitKind;

pub(crate) const NOT_COMMITTED: i32 = 1020;
pub(crate) const COMMIT_UNKNOWN_RESULT: i32 = 1021;
pub(crate) const TRANSACTION_TOO_LARGE: i32 = 2101;
pub(crate) const KEY_TOO_LARGE: i32 = 2102;
pub(crate) const VALUE_TOO_LARGE: i32 = 2103;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ErrorDisposition {
    Conflict,
    Unknown,
    Limit(LimitKind),
    Unavailable,
}

pub(crate) fn classify_error(code: i32, maybe_committed: bool) -> ErrorDisposition {
    if maybe_committed || code == COMMIT_UNKNOWN_RESULT {
        return ErrorDisposition::Unknown;
    }
    match code {
        NOT_COMMITTED => ErrorDisposition::Conflict,
        TRANSACTION_TOO_LARGE => ErrorDisposition::Limit(LimitKind::TransactionBytes),
        KEY_TOO_LARGE => ErrorDisposition::Limit(LimitKind::KeyBytes),
        VALUE_TOO_LARGE => ErrorDisposition::Limit(LimitKind::ValueBytes),
        _ => ErrorDisposition::Unavailable,
    }
}
