/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#define _GNU_SOURCE

#include "fdb_commit_unknown_fixture_api.h"

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

enum {
    UNKNOWN_RESULT = 1021,
    REAL_ERROR = 1031,
};

static const uint8_t TARGET_KEY[] = "target";
static const uint8_t OTHER_KEY[] = "other";
static const uint8_t VALUE[] = "value";

static void fail(const char *message) {
    fprintf(stderr, "fixture failure: %s\n", message);
    exit(1);
}

static void expect_result(const char *step, int actual, int expected) {
    if (actual != expected) {
        fprintf(stderr, "fixture failure: %s returned %d, expected %d\n", step,
                actual, expected);
        exit(1);
    }
}

static const char *fixture_mutation(void) {
    const char *kind = getenv("FIXTURE_MUTATION");
    return kind != NULL ? kind : "set";
}

static void mutate(FDBTransaction *transaction, const uint8_t *key,
                   size_t key_length) {
    const char *kind = fixture_mutation();
    if (strcmp(kind, "set") == 0) {
        fdb_transaction_set(transaction, key, (int)key_length, VALUE,
                            (int)(sizeof(VALUE) - 1));
    } else if (strcmp(kind, "clear") == 0) {
        fdb_transaction_clear(transaction, key, (int)key_length);
    } else if (strcmp(kind, "clear_range") == 0) {
        static const uint8_t begin[] = "targe";
        static const uint8_t end[] = "targetz";
        fdb_transaction_clear_range(transaction, begin, (int)(sizeof(begin) - 1),
                                    end, (int)(sizeof(end) - 1));
    } else if (strcmp(kind, "atomic") == 0) {
        fdb_transaction_atomic_op(transaction, key, (int)key_length, VALUE,
                                  (int)(sizeof(VALUE) - 1), 2);
    } else {
        fail("unknown FIXTURE_MUTATION");
    }
}

static FDBFuture *commit(FDBTransaction *transaction) {
    FDBFuture *future = fdb_transaction_commit(transaction);
    if (future == NULL) {
        fail("fake commit returned a null future");
    }
    return future;
}

static void scenario_single(bool injected) {
    FDBTransaction transaction = {.commit_error = 0, .identity = 1};
    mutate(&transaction, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    FDBFuture *future = commit(&transaction);
    expect_result("first future_get_error", fdb_future_get_error(future),
                  injected ? UNKNOWN_RESULT : 0);
    expect_result("second future_get_error", fdb_future_get_error(future), 0);
    fdb_future_destroy(future);
}

static void scenario_nonmatch(void) {
    FDBTransaction transaction = {.commit_error = 0, .identity = 2};
    mutate(&transaction, OTHER_KEY, sizeof(OTHER_KEY) - 1);
    FDBFuture *future = commit(&transaction);
    expect_result("nonmatching future_get_error", fdb_future_get_error(future),
                  0);
    fdb_future_destroy(future);
}

static void scenario_real_error(void) {
    FDBTransaction transaction = {.commit_error = REAL_ERROR, .identity = 3};
    mutate(&transaction, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    FDBFuture *future = commit(&transaction);
    expect_result("real-error future_get_error", fdb_future_get_error(future),
                  REAL_ERROR);
    fdb_future_destroy(future);
}

static void scenario_duplicate(void) {
    FDBTransaction first = {.commit_error = 0, .identity = 4};
    mutate(&first, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    FDBFuture *first_future = commit(&first);
    expect_result("first duplicate future", fdb_future_get_error(first_future),
                  UNKNOWN_RESULT);
    fdb_future_destroy(first_future);

    FDBTransaction second = {.commit_error = 0, .identity = 5};
    mutate(&second, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    FDBFuture *second_future = commit(&second);
    expect_result("second duplicate future", fdb_future_get_error(second_future),
                  0);
    fdb_future_destroy(second_future);
}

static void scenario_destroy_before_observation(void) {
    FDBTransaction transaction = {.commit_error = 0, .identity = 6};
    mutate(&transaction, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    fdb_future_destroy(commit(&transaction));
}

static void set_arm_descriptor(int descriptor) {
    char encoded[32];
    int length = snprintf(encoded, sizeof(encoded), "%d", descriptor);
    if (length < 0 || (size_t)length >= sizeof(encoded) ||
        setenv("NOKV_FDB_UNKNOWN_ARM_FD", encoded, 1) != 0) {
        fail("cannot publish fixture arm descriptor");
    }
}

static void write_arm_message(int descriptor) {
    const char *nonce = getenv("NOKV_FDB_UNKNOWN_RUN_NONCE");
    if (nonce == NULL) {
        fail("armed scenario has no run nonce");
    }
    char message[96];
    int length = snprintf(message, sizeof(message), "arm-v1:%s\n", nonce);
    if (length < 0 || (size_t)length >= sizeof(message) ||
        write(descriptor, message, (size_t)length) != length) {
        fail("cannot write arm message");
    }
}

static void scenario_armed(bool duplicate_arm) {
    int descriptors[2];
    if (pipe(descriptors) != 0) {
        fail("cannot create arm pipe");
    }
    set_arm_descriptor(descriptors[0]);

    if (!duplicate_arm) {
        FDBTransaction prearm = {.commit_error = 0, .identity = 7};
        mutate(&prearm, TARGET_KEY, sizeof(TARGET_KEY) - 1);
        FDBFuture *prearm_future = commit(&prearm);
        expect_result("pre-arm future", fdb_future_get_error(prearm_future), 0);
        fdb_future_destroy(prearm_future);
    }

    write_arm_message(descriptors[1]);
    if (duplicate_arm) {
        write_arm_message(descriptors[1]);
    }
    FDBTransaction target = {.commit_error = 0, .identity = 8};
    mutate(&target, TARGET_KEY, sizeof(TARGET_KEY) - 1);
    FDBFuture *target_future = commit(&target);
    expect_result("armed target future", fdb_future_get_error(target_future),
                  UNKNOWN_RESULT);
    fdb_future_destroy(target_future);
    close(descriptors[0]);
    close(descriptors[1]);
}

typedef struct ThreadCase {
    pthread_barrier_t *barrier;
    const uint8_t *key;
    size_t key_length;
    unsigned int identity;
    int result;
} ThreadCase;

static void *thread_commit(void *opaque) {
    ThreadCase *test = opaque;
    FDBTransaction transaction = {
        .commit_error = 0,
        .identity = test->identity,
    };
    int barrier_result = pthread_barrier_wait(test->barrier);
    if (barrier_result != 0 && barrier_result != PTHREAD_BARRIER_SERIAL_THREAD) {
        fail("thread barrier failed");
    }
    mutate(&transaction, test->key, test->key_length);
    FDBFuture *future = commit(&transaction);
    test->result = fdb_future_get_error(future);
    fdb_future_destroy(future);
    return NULL;
}

static void scenario_threaded(void) {
    pthread_barrier_t barrier;
    if (pthread_barrier_init(&barrier, NULL, 2) != 0) {
        fail("cannot initialize thread barrier");
    }
    ThreadCase target = {
        .barrier = &barrier,
        .key = TARGET_KEY,
        .key_length = sizeof(TARGET_KEY) - 1,
        .identity = 9,
        .result = -1,
    };
    ThreadCase other = {
        .barrier = &barrier,
        .key = OTHER_KEY,
        .key_length = sizeof(OTHER_KEY) - 1,
        .identity = 10,
        .result = -1,
    };
    pthread_t target_thread;
    pthread_t other_thread;
    if (pthread_create(&target_thread, NULL, thread_commit, &target) != 0 ||
        pthread_create(&other_thread, NULL, thread_commit, &other) != 0) {
        fail("cannot create fixture threads");
    }
    if (pthread_join(target_thread, NULL) != 0 ||
        pthread_join(other_thread, NULL) != 0) {
        fail("cannot join fixture threads");
    }
    expect_result("threaded target", target.result, UNKNOWN_RESULT);
    expect_result("threaded non-target", other.result, 0);
    pthread_barrier_destroy(&barrier);
}

int main(int argument_count, char **arguments) {
    if (argument_count != 2) {
        fail("expected exactly one scenario argument");
    }
    const char *scenario = arguments[1];
    if (strcmp(scenario, "transparent") == 0) {
        scenario_single(false);
    } else if (strcmp(scenario, "single") == 0) {
        scenario_single(true);
    } else if (strcmp(scenario, "nonmatch") == 0) {
        scenario_nonmatch();
    } else if (strcmp(scenario, "real-error") == 0) {
        scenario_real_error();
    } else if (strcmp(scenario, "duplicate") == 0) {
        scenario_duplicate();
    } else if (strcmp(scenario, "destroy") == 0) {
        scenario_destroy_before_observation();
    } else if (strcmp(scenario, "armed") == 0) {
        scenario_armed(false);
    } else if (strcmp(scenario, "duplicate-arm") == 0) {
        scenario_armed(true);
    } else if (strcmp(scenario, "threaded") == 0) {
        scenario_threaded();
    } else {
        fail("unknown scenario");
    }
    return 0;
}
