/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef NOKV_FDB_COMMIT_UNKNOWN_FIXTURE_API_H
#define NOKV_FDB_COMMIT_UNKNOWN_FIXTURE_API_H

#include <stdint.h>

typedef struct FDB_transaction {
    int commit_error;
    unsigned int identity;
} FDBTransaction;

typedef struct FDB_future FDBFuture;
typedef int fdb_error_t;
typedef int FDBMutationType;

void fdb_transaction_set(FDBTransaction *transaction, const uint8_t *key,
                         int key_length, const uint8_t *value,
                         int value_length);
void fdb_transaction_clear(FDBTransaction *transaction, const uint8_t *key,
                           int key_length);
void fdb_transaction_clear_range(FDBTransaction *transaction,
                                 const uint8_t *begin, int begin_length,
                                 const uint8_t *end, int end_length);
void fdb_transaction_atomic_op(FDBTransaction *transaction, const uint8_t *key,
                               int key_length, const uint8_t *parameter,
                               int parameter_length,
                               FDBMutationType operation_type);
FDBFuture *fdb_transaction_commit(FDBTransaction *transaction);
fdb_error_t fdb_future_get_error(FDBFuture *future);
void fdb_future_destroy(FDBFuture *future);

#endif
