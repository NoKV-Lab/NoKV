/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "fdb_commit_unknown_fixture_api.h"

#include <stdatomic.h>
#include <stdlib.h>

struct FDB_future {
    int error;
    unsigned int transaction_identity;
};

static atomic_uint g_commit_count;

void fdb_transaction_set(FDBTransaction *transaction, const uint8_t *key,
                         int key_length, const uint8_t *value,
                         int value_length) {
    (void)transaction;
    (void)key;
    (void)key_length;
    (void)value;
    (void)value_length;
}

void fdb_transaction_clear(FDBTransaction *transaction, const uint8_t *key,
                           int key_length) {
    (void)transaction;
    (void)key;
    (void)key_length;
}

void fdb_transaction_clear_range(FDBTransaction *transaction,
                                 const uint8_t *begin, int begin_length,
                                 const uint8_t *end, int end_length) {
    (void)transaction;
    (void)begin;
    (void)begin_length;
    (void)end;
    (void)end_length;
}

void fdb_transaction_atomic_op(FDBTransaction *transaction, const uint8_t *key,
                               int key_length, const uint8_t *parameter,
                               int parameter_length,
                               FDBMutationType operation_type) {
    (void)transaction;
    (void)key;
    (void)key_length;
    (void)parameter;
    (void)parameter_length;
    (void)operation_type;
}

FDBFuture *fdb_transaction_commit(FDBTransaction *transaction) {
    FDBFuture *future = malloc(sizeof(*future));
    if (future == NULL) {
        abort();
    }
    future->error = transaction->commit_error;
    future->transaction_identity = transaction->identity;
    atomic_fetch_add_explicit(&g_commit_count, 1U, memory_order_relaxed);
    return future;
}

fdb_error_t fdb_future_get_error(FDBFuture *future) {
    return future->error;
}

void fdb_future_destroy(FDBFuture *future) {
    free(future);
}
