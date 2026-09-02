/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

// The FDB release binary intentionally exposes the same CLI surface with an
// additional compile-time provider. Keeping one implementation prevents the
// two binaries from drifting while still giving Cargo distinct target paths.
include!("main.rs");
