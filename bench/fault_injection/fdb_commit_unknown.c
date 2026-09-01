/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

#define _GNU_SOURCE

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct FDB_transaction FDBTransaction;
typedef struct FDB_future FDBFuture;
typedef int fdb_error_t;
typedef int FDBMutationType;

/*
 * Shim-only selector contract. Product binaries must never read these values:
 *
 *   NOKV_FDB_UNKNOWN_V1=1
 *   NOKV_FDB_UNKNOWN_RUN_NONCE=<bounded [A-Za-z0-9_-]+>
 *   NOKV_FDB_UNKNOWN_TARGET_KEY_HEX=<lowercase exact key bytes>
 *   NOKV_FDB_UNKNOWN_MUTATION=set|clear|clear_range|atomic
 *   NOKV_FDB_UNKNOWN_MODE=ordinal|armed
 *   NOKV_FDB_UNKNOWN_ORDINAL=<one-based match>       (ordinal mode)
 *   NOKV_FDB_UNKNOWN_EXPECTED_MATCHES=<positive total exact-key matches>
 *   NOKV_FDB_UNKNOWN_ARM_FD=<private read descriptor> (armed mode)
 *   NOKV_FDB_UNKNOWN_EVENT_FD=<private write descriptor>
 */

enum {
    FDB_COMMIT_UNKNOWN_RESULT = 1021,
    MAX_TARGET_KEY_BYTES = 16384,
    MAX_TARGET_KEY_HEX_BYTES = MAX_TARGET_KEY_BYTES * 2,
    MAX_NONCE_BYTES = 64,
    MAX_ARM_MESSAGE_BYTES = 256,
    EVENT_BUFFER_BYTES = 2048,
};

typedef enum MutationKind {
    MUTATION_SET,
    MUTATION_CLEAR,
    MUTATION_CLEAR_RANGE,
    MUTATION_ATOMIC,
    MUTATION_INVALID,
} MutationKind;

typedef enum SelectionMode {
    SELECTION_ORDINAL,
    SELECTION_ARMED,
    SELECTION_INVALID,
} SelectionMode;

typedef struct Sha256 {
    uint32_t state[8];
    uint64_t bit_length;
    uint8_t block[64];
    size_t block_length;
} Sha256;

typedef struct InjectorState {
    pthread_mutex_t mutex;
    bool enabled;
    bool active;
    bool invalid;
    bool armed;
    bool arm_consumed;
    bool selection_closed;
    bool target_future_observed;
    bool target_future_substituted;
    MutationKind kind;
    SelectionMode mode;
    uint8_t target_key[MAX_TARGET_KEY_BYTES];
    size_t target_key_length;
    char nonce[MAX_NONCE_BYTES + 1];
    char selector_sha256[65];
    char target_key_sha256[65];
    uint64_t ordinal;
    uint64_t expected_matches;
    uint64_t matching_mutations;
    uint64_t prearm_matches;
    uint64_t postselection_matches;
    uint64_t selected_transactions;
    uint64_t target_commits;
    uint64_t substitutions;
    uint64_t duplicate_matches;
    uint64_t arm_messages;
    uint64_t event_writes;
    int event_fd;
    int arm_fd;
    FDBTransaction *target_transaction;
    FDBFuture *target_future;
} InjectorState;

typedef struct EventSnapshot {
    char event[40];
    char nonce[MAX_NONCE_BYTES + 1];
    char selector_sha256[65];
    char target_key_sha256[65];
    const char *kind;
    const char *mode;
    uint64_t expected_matches;
    uint64_t matching_mutations;
    uint64_t prearm_matches;
    uint64_t postselection_matches;
    uint64_t selected_transactions;
    uint64_t target_commits;
    uint64_t substitutions;
    uint64_t duplicate_matches;
    uint64_t arm_messages;
    uint64_t event_writes;
    int real_result;
    int substituted_result;
    bool invalid;
    int event_fd;
} EventSnapshot;

static InjectorState g_state = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .event_fd = -1,
    .arm_fd = -1,
};
static pthread_once_t g_once = PTHREAD_ONCE_INIT;

static void (*real_transaction_set)(FDBTransaction *, const uint8_t *, int,
                                    const uint8_t *, int);
static void (*real_transaction_clear)(FDBTransaction *, const uint8_t *, int);
static void (*real_transaction_clear_range)(FDBTransaction *, const uint8_t *,
                                            int, const uint8_t *, int);
static void (*real_transaction_atomic_op)(FDBTransaction *, const uint8_t *,
                                          int, const uint8_t *, int,
                                          FDBMutationType);
static FDBFuture *(*real_transaction_commit)(FDBTransaction *);
static fdb_error_t (*real_future_get_error)(FDBFuture *);
static void (*real_future_destroy)(FDBFuture *);

static uint32_t rotate_right(uint32_t value, uint32_t amount) {
    return (value >> amount) | (value << (32U - amount));
}

static void sha256_transform(Sha256 *sha, const uint8_t block[64]) {
    static const uint32_t constants[64] = {
        0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
        0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
        0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
        0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
        0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
        0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
        0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
        0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
        0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
        0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
        0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
        0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
        0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
        0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
        0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
        0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
    };
    uint32_t words[64];
    for (size_t index = 0; index < 16; ++index) {
        size_t offset = index * 4;
        words[index] = ((uint32_t)block[offset] << 24U) |
                       ((uint32_t)block[offset + 1] << 16U) |
                       ((uint32_t)block[offset + 2] << 8U) |
                       (uint32_t)block[offset + 3];
    }
    for (size_t index = 16; index < 64; ++index) {
        uint32_t s0 = rotate_right(words[index - 15], 7U) ^
                      rotate_right(words[index - 15], 18U) ^
                      (words[index - 15] >> 3U);
        uint32_t s1 = rotate_right(words[index - 2], 17U) ^
                      rotate_right(words[index - 2], 19U) ^
                      (words[index - 2] >> 10U);
        words[index] = words[index - 16] + s0 + words[index - 7] + s1;
    }

    uint32_t a = sha->state[0];
    uint32_t b = sha->state[1];
    uint32_t c = sha->state[2];
    uint32_t d = sha->state[3];
    uint32_t e = sha->state[4];
    uint32_t f = sha->state[5];
    uint32_t g = sha->state[6];
    uint32_t h = sha->state[7];
    for (size_t index = 0; index < 64; ++index) {
        uint32_t sum1 = rotate_right(e, 6U) ^ rotate_right(e, 11U) ^
                        rotate_right(e, 25U);
        uint32_t choose = (e & f) ^ ((~e) & g);
        uint32_t temporary1 = h + sum1 + choose + constants[index] + words[index];
        uint32_t sum0 = rotate_right(a, 2U) ^ rotate_right(a, 13U) ^
                        rotate_right(a, 22U);
        uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        uint32_t temporary2 = sum0 + majority;
        h = g;
        g = f;
        f = e;
        e = d + temporary1;
        d = c;
        c = b;
        b = a;
        a = temporary1 + temporary2;
    }
    sha->state[0] += a;
    sha->state[1] += b;
    sha->state[2] += c;
    sha->state[3] += d;
    sha->state[4] += e;
    sha->state[5] += f;
    sha->state[6] += g;
    sha->state[7] += h;
}

static void sha256_init(Sha256 *sha) {
    *sha = (Sha256){
        .state = {0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U,
                  0xa54ff53aU, 0x510e527fU, 0x9b05688cU,
                  0x1f83d9abU, 0x5be0cd19U},
    };
}

static void sha256_update(Sha256 *sha, const uint8_t *bytes, size_t length) {
    for (size_t index = 0; index < length; ++index) {
        sha->block[sha->block_length++] = bytes[index];
        if (sha->block_length == sizeof(sha->block)) {
            sha256_transform(sha, sha->block);
            sha->bit_length += 512U;
            sha->block_length = 0;
        }
    }
}

static void sha256_finish(Sha256 *sha, uint8_t digest[32]) {
    sha->bit_length += (uint64_t)sha->block_length * 8U;
    sha->block[sha->block_length++] = 0x80U;
    if (sha->block_length > 56) {
        while (sha->block_length < 64) {
            sha->block[sha->block_length++] = 0;
        }
        sha256_transform(sha, sha->block);
        sha->block_length = 0;
    }
    while (sha->block_length < 56) {
        sha->block[sha->block_length++] = 0;
    }
    for (size_t index = 0; index < 8; ++index) {
        sha->block[63 - index] = (uint8_t)(sha->bit_length >> (index * 8U));
    }
    sha256_transform(sha, sha->block);
    for (size_t index = 0; index < 8; ++index) {
        digest[index * 4] = (uint8_t)(sha->state[index] >> 24U);
        digest[index * 4 + 1] = (uint8_t)(sha->state[index] >> 16U);
        digest[index * 4 + 2] = (uint8_t)(sha->state[index] >> 8U);
        digest[index * 4 + 3] = (uint8_t)sha->state[index];
    }
}

static void digest_hex(const uint8_t *bytes, size_t length, char output[65]) {
    static const char hex[] = "0123456789abcdef";
    Sha256 sha;
    uint8_t digest[32];
    sha256_init(&sha);
    sha256_update(&sha, bytes, length);
    sha256_finish(&sha, digest);
    for (size_t index = 0; index < sizeof(digest); ++index) {
        output[index * 2] = hex[digest[index] >> 4U];
        output[index * 2 + 1] = hex[digest[index] & 0x0fU];
    }
    output[64] = '\0';
}

static const char *kind_name(MutationKind kind) {
    switch (kind) {
    case MUTATION_SET:
        return "set";
    case MUTATION_CLEAR:
        return "clear";
    case MUTATION_CLEAR_RANGE:
        return "clear_range";
    case MUTATION_ATOMIC:
        return "atomic";
    default:
        return "invalid";
    }
}

static const char *mode_name(SelectionMode mode) {
    switch (mode) {
    case SELECTION_ORDINAL:
        return "ordinal";
    case SELECTION_ARMED:
        return "armed";
    default:
        return "invalid";
    }
}

static MutationKind parse_kind(const char *value) {
    if (value != NULL && strcmp(value, "set") == 0) {
        return MUTATION_SET;
    }
    if (value != NULL && strcmp(value, "clear") == 0) {
        return MUTATION_CLEAR;
    }
    if (value != NULL && strcmp(value, "clear_range") == 0) {
        return MUTATION_CLEAR_RANGE;
    }
    if (value != NULL && strcmp(value, "atomic") == 0) {
        return MUTATION_ATOMIC;
    }
    return MUTATION_INVALID;
}

static SelectionMode parse_mode(const char *value) {
    if (value != NULL && strcmp(value, "ordinal") == 0) {
        return SELECTION_ORDINAL;
    }
    if (value != NULL && strcmp(value, "armed") == 0) {
        return SELECTION_ARMED;
    }
    return SELECTION_INVALID;
}

static bool parse_u64(const char *value, uint64_t *output) {
    if (value == NULL || *value == '\0' || *value == '-') {
        return false;
    }
    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        return false;
    }
    *output = (uint64_t)parsed;
    return true;
}

static bool parse_fd(const char *value, int *output) {
    uint64_t parsed = 0;
    if (!parse_u64(value, &parsed) || parsed > INT32_MAX) {
        return false;
    }
    *output = (int)parsed;
    return true;
}

static bool valid_nonce(const char *nonce) {
    if (nonce == NULL) {
        return false;
    }
    size_t length = strlen(nonce);
    if (length == 0 || length > MAX_NONCE_BYTES) {
        return false;
    }
    for (size_t index = 0; index < length; ++index) {
        char byte = nonce[index];
        if (!((byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
              (byte >= '0' && byte <= '9') || byte == '-' || byte == '_')) {
            return false;
        }
    }
    return true;
}

static int hex_nibble(char byte) {
    if (byte >= '0' && byte <= '9') {
        return byte - '0';
    }
    if (byte >= 'a' && byte <= 'f') {
        return byte - 'a' + 10;
    }
    return -1;
}

static bool decode_key(const char *hex, uint8_t *output, size_t *output_length) {
    if (hex == NULL) {
        return false;
    }
    size_t length = strlen(hex);
    if (length == 0 || length > MAX_TARGET_KEY_HEX_BYTES || (length % 2) != 0) {
        return false;
    }
    for (size_t index = 0; index < length / 2; ++index) {
        int high = hex_nibble(hex[index * 2]);
        int low = hex_nibble(hex[index * 2 + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        output[index] = (uint8_t)((high << 4) | low);
    }
    *output_length = length / 2;
    return true;
}

static void load_symbol(void *destination, size_t destination_size,
                        const char *name) {
    void *symbol = dlsym(RTLD_NEXT, name);
    if (symbol == NULL || destination_size != sizeof(symbol)) {
        g_state.invalid = true;
        return;
    }
    memcpy(destination, &symbol, sizeof(symbol));
}

static void initialize(void) {
    load_symbol(&real_transaction_set, sizeof(real_transaction_set),
                "fdb_transaction_set");
    load_symbol(&real_transaction_clear, sizeof(real_transaction_clear),
                "fdb_transaction_clear");
    load_symbol(&real_transaction_clear_range,
                sizeof(real_transaction_clear_range),
                "fdb_transaction_clear_range");
    load_symbol(&real_transaction_atomic_op, sizeof(real_transaction_atomic_op),
                "fdb_transaction_atomic_op");
    load_symbol(&real_transaction_commit, sizeof(real_transaction_commit),
                "fdb_transaction_commit");
    load_symbol(&real_future_get_error, sizeof(real_future_get_error),
                "fdb_future_get_error");
    load_symbol(&real_future_destroy, sizeof(real_future_destroy),
                "fdb_future_destroy");

    const char *version = getenv("NOKV_FDB_UNKNOWN_V1");
    if (version == NULL) {
        return;
    }
    g_state.enabled = true;
    if (strcmp(version, "1") != 0) {
        g_state.invalid = true;
    }

    const char *nonce = getenv("NOKV_FDB_UNKNOWN_RUN_NONCE");
    if (!valid_nonce(nonce)) {
        g_state.invalid = true;
    } else {
        memcpy(g_state.nonce, nonce, strlen(nonce) + 1);
    }
    g_state.kind = parse_kind(getenv("NOKV_FDB_UNKNOWN_MUTATION"));
    g_state.mode = parse_mode(getenv("NOKV_FDB_UNKNOWN_MODE"));
    if (g_state.kind == MUTATION_INVALID || g_state.mode == SELECTION_INVALID) {
        g_state.invalid = true;
    }
    if (!decode_key(getenv("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX"),
                    g_state.target_key, &g_state.target_key_length)) {
        g_state.invalid = true;
    }
    if (!parse_fd(getenv("NOKV_FDB_UNKNOWN_EVENT_FD"), &g_state.event_fd)) {
        g_state.invalid = true;
    }
    if (!parse_u64(getenv("NOKV_FDB_UNKNOWN_EXPECTED_MATCHES"),
                   &g_state.expected_matches) ||
        g_state.expected_matches == 0 ||
        g_state.expected_matches > 1000000U) {
        g_state.invalid = true;
    }

    if (g_state.mode == SELECTION_ORDINAL) {
        if (!parse_u64(getenv("NOKV_FDB_UNKNOWN_ORDINAL"), &g_state.ordinal) ||
            g_state.ordinal == 0 || g_state.ordinal > 1000000U ||
            g_state.ordinal > g_state.expected_matches) {
            g_state.invalid = true;
        }
    } else if (g_state.mode == SELECTION_ARMED) {
        if (!parse_fd(getenv("NOKV_FDB_UNKNOWN_ARM_FD"), &g_state.arm_fd)) {
            g_state.invalid = true;
        } else {
            int flags = fcntl(g_state.arm_fd, F_GETFL, 0);
            if (flags < 0 || fcntl(g_state.arm_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
                g_state.invalid = true;
            }
        }
    }

    digest_hex(g_state.target_key, g_state.target_key_length,
               g_state.target_key_sha256);
    char canonical[MAX_TARGET_KEY_HEX_BYTES + 512];
    int canonical_length = snprintf(
        canonical, sizeof(canonical),
        "nokv-fdb-unknown-selector-v2\nnonce=%s\nkind=%s\nkey=%s\nmode=%s\nordinal=%llu\nexpected_matches=%llu\n",
        g_state.nonce, kind_name(g_state.kind),
        getenv("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX") != NULL
            ? getenv("NOKV_FDB_UNKNOWN_TARGET_KEY_HEX")
            : "",
        mode_name(g_state.mode), (unsigned long long)g_state.ordinal,
        (unsigned long long)g_state.expected_matches);
    if (canonical_length < 0 || (size_t)canonical_length >= sizeof(canonical)) {
        g_state.invalid = true;
    } else {
        digest_hex((const uint8_t *)canonical, (size_t)canonical_length,
                   g_state.selector_sha256);
    }
    memset(canonical, 0, sizeof(canonical));
    g_state.active = !g_state.invalid;
}

static void ensure_initialized(void) {
    (void)pthread_once(&g_once, initialize);
}

static int compare_bytes(const uint8_t *left, size_t left_length,
                         const uint8_t *right, size_t right_length) {
    size_t common = left_length < right_length ? left_length : right_length;
    int compared = memcmp(left, right, common);
    if (compared != 0) {
        return compared;
    }
    if (left_length < right_length) {
        return -1;
    }
    if (left_length > right_length) {
        return 1;
    }
    return 0;
}

static bool key_matches(const uint8_t *key, int key_length) {
    return key != NULL && key_length >= 0 &&
           (size_t)key_length == g_state.target_key_length &&
           memcmp(key, g_state.target_key, g_state.target_key_length) == 0;
}

static bool range_contains_target(const uint8_t *begin, int begin_length,
                                  const uint8_t *end, int end_length) {
    if (begin == NULL || end == NULL || begin_length < 0 || end_length < 0) {
        return false;
    }
    return compare_bytes(g_state.target_key, g_state.target_key_length, begin,
                         (size_t)begin_length) >= 0 &&
           compare_bytes(g_state.target_key, g_state.target_key_length, end,
                         (size_t)end_length) < 0;
}

static void consume_arm_messages_locked(void) {
    if (g_state.mode != SELECTION_ARMED || g_state.arm_fd < 0) {
        return;
    }
    char buffer[MAX_ARM_MESSAGE_BYTES + 1];
    for (;;) {
        ssize_t received = read(g_state.arm_fd, buffer, MAX_ARM_MESSAGE_BYTES);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                g_state.invalid = true;
            }
            break;
        }
        if (received == 0) {
            break;
        }
        buffer[received] = '\0';
        static const char prefix[] = "arm-v1:";
        char expected[MAX_NONCE_BYTES + sizeof(prefix) + 1];
        size_t nonce_length = strlen(g_state.nonce);
        size_t expected_length = sizeof(prefix) - 1 + nonce_length + 1;
        memcpy(expected, prefix, sizeof(prefix) - 1);
        memcpy(expected + sizeof(prefix) - 1, g_state.nonce, nonce_length);
        expected[expected_length - 1] = '\n';
        size_t offset = 0;
        while (offset < (size_t)received) {
            char *newline =
                memchr(buffer + offset, '\n', (size_t)received - offset);
            if (newline == NULL) {
                g_state.invalid = true;
                break;
            }
            size_t line_length = (size_t)(newline - (buffer + offset)) + 1;
            ++g_state.arm_messages;
            if (line_length != expected_length ||
                memcmp(buffer + offset, expected, line_length) != 0 ||
                g_state.arm_consumed || g_state.armed) {
                g_state.invalid = true;
            } else {
                g_state.armed = true;
            }
            offset += line_length;
        }
        memset(expected, 0, sizeof(expected));
    }
    memset(buffer, 0, sizeof(buffer));
}

static void record_match_locked(FDBTransaction *transaction) {
    ++g_state.matching_mutations;
    if (g_state.matching_mutations > g_state.expected_matches) {
        ++g_state.duplicate_matches;
        g_state.invalid = true;
        return;
    }
    bool select = false;
    if (g_state.mode == SELECTION_ORDINAL) {
        select = g_state.matching_mutations == g_state.ordinal;
    } else if (g_state.mode == SELECTION_ARMED) {
        consume_arm_messages_locked();
        if (g_state.armed) {
            select = true;
            g_state.armed = false;
            g_state.arm_consumed = true;
        } else {
            ++g_state.prearm_matches;
        }
    }

    if (select) {
        if (g_state.target_transaction != NULL || g_state.selection_closed) {
            ++g_state.duplicate_matches;
            g_state.invalid = true;
        } else {
            g_state.target_transaction = transaction;
            ++g_state.selected_transactions;
        }
    } else if (g_state.target_transaction != NULL) {
        ++g_state.duplicate_matches;
        g_state.invalid = true;
    } else if (g_state.selection_closed) {
        ++g_state.postselection_matches;
    }
}

static void record_mutation(MutationKind kind, FDBTransaction *transaction,
                            const uint8_t *key, int key_length,
                            const uint8_t *end, int end_length) {
    ensure_initialized();
    if (!g_state.active || kind != g_state.kind) {
        return;
    }
    bool matches = kind == MUTATION_CLEAR_RANGE
                       ? range_contains_target(key, key_length, end, end_length)
                       : key_matches(key, key_length);
    if (!matches) {
        return;
    }
    pthread_mutex_lock(&g_state.mutex);
    record_match_locked(transaction);
    pthread_mutex_unlock(&g_state.mutex);
}

static void capture_event_locked(EventSnapshot *snapshot, const char *event,
                                 int real_result, int substituted_result) {
    memset(snapshot, 0, sizeof(*snapshot));
    size_t event_length = strnlen(event, sizeof(snapshot->event) - 1);
    memcpy(snapshot->event, event, event_length);
    memcpy(snapshot->nonce, g_state.nonce, sizeof(snapshot->nonce));
    memcpy(snapshot->selector_sha256, g_state.selector_sha256,
           sizeof(snapshot->selector_sha256));
    memcpy(snapshot->target_key_sha256, g_state.target_key_sha256,
           sizeof(snapshot->target_key_sha256));
    snapshot->kind = kind_name(g_state.kind);
    snapshot->mode = mode_name(g_state.mode);
    snapshot->expected_matches = g_state.expected_matches;
    snapshot->matching_mutations = g_state.matching_mutations;
    snapshot->prearm_matches = g_state.prearm_matches;
    snapshot->postselection_matches = g_state.postselection_matches;
    snapshot->selected_transactions = g_state.selected_transactions;
    snapshot->target_commits = g_state.target_commits;
    snapshot->substitutions = g_state.substitutions;
    snapshot->duplicate_matches = g_state.duplicate_matches;
    snapshot->arm_messages = g_state.arm_messages;
    snapshot->event_writes = g_state.event_writes;
    snapshot->real_result = real_result;
    snapshot->substituted_result = substituted_result;
    snapshot->invalid = g_state.invalid;
    snapshot->event_fd = g_state.event_fd;
}

static bool write_all(int descriptor, const char *bytes, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t written = write(descriptor, bytes + offset, length - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return false;
        }
        offset += (size_t)written;
    }
    return true;
}

static void emit_event(const EventSnapshot *snapshot) {
    char buffer[EVENT_BUFFER_BYTES];
    long thread_id = (long)syscall(SYS_gettid);
    int length = snprintf(
        buffer, sizeof(buffer),
        "{\"version\":1,\"event\":\"%s\",\"nonce\":\"%s\",\"pid\":%ld,\"tid\":%ld,"
        "\"selector_sha256\":\"%s\",\"target_key_sha256\":\"%s\","
        "\"kind\":\"%s\",\"mode\":\"%s\",\"expected_matches\":%llu,"
        "\"matching_mutations\":%llu,\"prearm_matches\":%llu,"
        "\"postselection_matches\":%llu,\"selected_transactions\":%llu,"
        "\"target_commits\":%llu,\"substitutions\":%llu,"
        "\"duplicate_matches\":%llu,\"arm_messages\":%llu,"
        "\"event_writes_before\":%llu,\"real_result\":%d,"
        "\"substituted_result\":%d,\"invalid\":%s}\n",
        snapshot->event, snapshot->nonce, (long)getpid(), thread_id,
        snapshot->selector_sha256, snapshot->target_key_sha256, snapshot->kind,
        snapshot->mode, (unsigned long long)snapshot->expected_matches,
        (unsigned long long)snapshot->matching_mutations,
        (unsigned long long)snapshot->prearm_matches,
        (unsigned long long)snapshot->postselection_matches,
        (unsigned long long)snapshot->selected_transactions,
        (unsigned long long)snapshot->target_commits,
        (unsigned long long)snapshot->substitutions,
        (unsigned long long)snapshot->duplicate_matches,
        (unsigned long long)snapshot->arm_messages,
        (unsigned long long)snapshot->event_writes, snapshot->real_result,
        snapshot->substituted_result, snapshot->invalid ? "true" : "false");
    bool success = length >= 0 && (size_t)length < sizeof(buffer) &&
                   snapshot->event_fd >= 0 &&
                   write_all(snapshot->event_fd, buffer, (size_t)length);
    pthread_mutex_lock(&g_state.mutex);
    if (success) {
        ++g_state.event_writes;
    } else {
        g_state.invalid = true;
    }
    pthread_mutex_unlock(&g_state.mutex);
    memset(buffer, 0, sizeof(buffer));
}

void fdb_transaction_set(FDBTransaction *transaction, const uint8_t *key,
                         int key_length, const uint8_t *value,
                         int value_length) {
    ensure_initialized();
    if (real_transaction_set == NULL) {
        _exit(126);
    }
    real_transaction_set(transaction, key, key_length, value, value_length);
    record_mutation(MUTATION_SET, transaction, key, key_length, NULL, 0);
}

void fdb_transaction_clear(FDBTransaction *transaction, const uint8_t *key,
                           int key_length) {
    ensure_initialized();
    if (real_transaction_clear == NULL) {
        _exit(126);
    }
    real_transaction_clear(transaction, key, key_length);
    record_mutation(MUTATION_CLEAR, transaction, key, key_length, NULL, 0);
}

void fdb_transaction_clear_range(FDBTransaction *transaction,
                                 const uint8_t *begin, int begin_length,
                                 const uint8_t *end, int end_length) {
    ensure_initialized();
    if (real_transaction_clear_range == NULL) {
        _exit(126);
    }
    real_transaction_clear_range(transaction, begin, begin_length, end,
                                 end_length);
    record_mutation(MUTATION_CLEAR_RANGE, transaction, begin, begin_length, end,
                    end_length);
}

void fdb_transaction_atomic_op(FDBTransaction *transaction, const uint8_t *key,
                               int key_length, const uint8_t *parameter,
                               int parameter_length,
                               FDBMutationType operation_type) {
    ensure_initialized();
    if (real_transaction_atomic_op == NULL) {
        _exit(126);
    }
    real_transaction_atomic_op(transaction, key, key_length, parameter,
                               parameter_length, operation_type);
    record_mutation(MUTATION_ATOMIC, transaction, key, key_length, NULL, 0);
}

FDBFuture *fdb_transaction_commit(FDBTransaction *transaction) {
    ensure_initialized();
    if (real_transaction_commit == NULL) {
        _exit(126);
    }
    bool target = false;
    if (g_state.active) {
        pthread_mutex_lock(&g_state.mutex);
        if (transaction == g_state.target_transaction) {
            target = true;
            g_state.selection_closed = true;
            g_state.target_transaction = NULL;
            ++g_state.target_commits;
            if (g_state.target_commits != 1) {
                g_state.invalid = true;
            }
        }
        pthread_mutex_unlock(&g_state.mutex);
    }
    FDBFuture *future = real_transaction_commit(transaction);
    if (target) {
        pthread_mutex_lock(&g_state.mutex);
        if (future == NULL || g_state.target_future != NULL) {
            g_state.invalid = true;
        } else {
            g_state.target_future = future;
        }
        pthread_mutex_unlock(&g_state.mutex);
    }
    return future;
}

fdb_error_t fdb_future_get_error(FDBFuture *future) {
    ensure_initialized();
    if (real_future_get_error == NULL) {
        _exit(126);
    }
    fdb_error_t real_result = real_future_get_error(future);
    fdb_error_t result = real_result;
    EventSnapshot event;
    bool emit = false;
    if (g_state.active) {
        pthread_mutex_lock(&g_state.mutex);
        if (future == g_state.target_future) {
            g_state.target_future_observed = true;
            if (real_result == 0 && !g_state.target_future_substituted &&
                g_state.substitutions == 0) {
                g_state.target_future_substituted = true;
                ++g_state.substitutions;
                result = FDB_COMMIT_UNKNOWN_RESULT;
                capture_event_locked(&event, "substitution", real_result, result);
                emit = true;
            } else if (real_result != 0 && !g_state.target_future_substituted) {
                capture_event_locked(&event, "real_error_passthrough", real_result,
                                     real_result);
                emit = true;
            }
        }
        pthread_mutex_unlock(&g_state.mutex);
    }
    if (emit) {
        emit_event(&event);
    }
    return result;
}

void fdb_future_destroy(FDBFuture *future) {
    ensure_initialized();
    if (real_future_destroy == NULL) {
        _exit(126);
    }
    EventSnapshot event;
    bool emit = false;
    if (g_state.active) {
        pthread_mutex_lock(&g_state.mutex);
        if (future == g_state.target_future) {
            if (!g_state.target_future_observed) {
                g_state.invalid = true;
                capture_event_locked(&event, "destroyed_before_observation", -1,
                                     -1);
                emit = true;
            }
            g_state.target_future = NULL;
        }
        pthread_mutex_unlock(&g_state.mutex);
    }
    if (emit) {
        emit_event(&event);
    }
    real_future_destroy(future);
}

__attribute__((destructor)) static void finalize_injector(void) {
    ensure_initialized();
    if (!g_state.enabled) {
        return;
    }
    EventSnapshot summary;
    pthread_mutex_lock(&g_state.mutex);
    if (g_state.matching_mutations != g_state.expected_matches ||
        g_state.selected_transactions != 1 || g_state.target_commits != 1 ||
        g_state.substitutions != 1 || g_state.duplicate_matches != 0 ||
        g_state.target_future != NULL ||
        (g_state.mode == SELECTION_ARMED && g_state.arm_messages != 1)) {
        g_state.invalid = true;
    }
    capture_event_locked(&summary, "summary", 0, 0);
    pthread_mutex_unlock(&g_state.mutex);
    emit_event(&summary);

    volatile uint8_t *key = g_state.target_key;
    for (size_t index = 0; index < sizeof(g_state.target_key); ++index) {
        key[index] = 0;
    }
}
