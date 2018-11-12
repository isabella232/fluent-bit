/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#include <fluent-bit.h>
#include <fluent-bit/flb_pack.h>
#include "flb_tests_runtime.h"

pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;
char *output = NULL;

/* Test data */

/* Test functions */
void flb_test_filter_nest_single(void);
void flb_test_filter_nest_multi(void);
void flb_test_filter_lift(void);
void flb_test_filter_lift_add_prefix(void);

/* Test list */
TEST_LIST = {
    {"nest single", flb_test_filter_nest_single },
    {"nest multi", flb_test_filter_nest_multi },
    {"lift", flb_test_filter_lift },
    {"lift with add_prefix", flb_test_filter_lift_add_prefix },
    {NULL, NULL}
};


// normalize JSON by round-tripping it through a msgpack
char *normalize_json(char *json_str)
{
    char *buf;
    size_t buf_len;
    int root_type;
    msgpack_unpacked elem;
    size_t off = 0;
    size_t out_json_str_capacity = 500;
    char *out_json_str = malloc(out_json_str_capacity);
    size_t out_json_str_len = 0;

    if (flb_pack_json(json_str, strlen(json_str),
                &buf, &buf_len, &root_type)) {
        return NULL;
    }

    msgpack_unpacked_init(&elem);
    while (msgpack_unpack_next(&elem, buf, buf_len, &off)) {
        while (1) {
            if (out_json_str_len) {
                out_json_str[out_json_str_len++] = ',';
                out_json_str[out_json_str_len++] = ' ';
            }
            int chars_written = flb_msgpack_to_json(out_json_str + out_json_str_len, out_json_str_capacity - out_json_str_len, &elem.data);
            if (chars_written > 0) {
                out_json_str_len += chars_written;
                break;
            }
            out_json_str = realloc(out_json_str, out_json_str_capacity + 500);
        }
    }
    msgpack_unpacked_destroy(&elem);

    return out_json_str;
}

#define TEST_IF_EQUIVALENT_JSON(got_json_str, expected_json_str) \
{ \
    char *got_json_str_normalized = normalize_json(got_json_str); \
    char *expected_json_str_normalized = normalize_json(expected_json_str); \
     \
    if ( \
        TEST_CHECK_(got_json_str_normalized != NULL, "The got JSON can be normalized: '%s'", got_json_str) \
        && \
        TEST_CHECK_(expected_json_str_normalized != NULL, "The expected JSON can be normalized: '%s'", expected_json_str) \
        && \
        !TEST_CHECK_(!strcmp(got_json_str_normalized, expected_json_str_normalized), "JSON values match") \
    ) { \
        TEST_MSG(" Raw values:"); \
        TEST_MSG("    Got:      %s", got_json_str); \
        TEST_MSG("    Expected: %s", expected_json_str); \
        TEST_MSG(" Normalized values:"); \
        TEST_MSG("    Got:      %s", got_json_str_normalized); \
        TEST_MSG("    Expected: %s", expected_json_str_normalized); \
    } \
}

void set_output(char *val)
{
    pthread_mutex_lock(&result_mutex);
    output = val;
    pthread_mutex_unlock(&result_mutex);
}

char *get_output(void)
{
    char *val;

    pthread_mutex_lock(&result_mutex);
    val = output;
    pthread_mutex_unlock(&result_mutex);

    return val;
}

int callback_test(void* data, size_t size, void* cb_data)
{
    if (size > 0) {
        flb_debug("[test_filter_nest] received message: %s", data);
        set_output(data); /* success */
    }
    return 0;
}

void flb_test_filter_nest_single(void)
{
    int ret;
    int bytes;
    char *p, *output, *expected;
    flb_ctx_t *ctx;
    int in_ffd;
    int out_ffd;
    int filter_ffd;

    struct flb_lib_out_cb cb;
    cb.cb   = callback_test;
    cb.data = NULL;

    ctx = flb_create();

    in_ffd = flb_input(ctx, (char *) "lib", NULL);
    TEST_CHECK(in_ffd >= 0);
    flb_input_set(ctx, in_ffd, "tag", "test", NULL);

    out_ffd = flb_output(ctx, (char *) "lib", &cb);
    TEST_CHECK(out_ffd >= 0);
    flb_output_set(ctx, out_ffd,
        "format", "json",
        "match", "test",
        NULL);
    filter_ffd = flb_filter(ctx, (char *) "nest", NULL);
    TEST_CHECK(filter_ffd >= 0);

    ret = flb_filter_set(ctx, filter_ffd,
        "Match", "*",
        "Operation", "nest",
        "Wildcard", "to_nest",
        "Nest_under", "nested_key",
        NULL);

    TEST_CHECK(ret == 0);

    ret = flb_start(ctx);
    TEST_CHECK(ret == 0);

    p = "[ 1448403340, { \"to_nest\": \"This is the data to nest\", \"extra\": \"Some more data\" } ]";
    bytes = flb_lib_push(ctx, in_ffd, p, strlen(p));
    TEST_CHECK(bytes == strlen(p));

    /* waiting flush */
    do {
        output = get_output();
        sleep(1);
    } while (!output);

    TEST_CHECK_(output != NULL, "Expected output to not be NULL");

    if (output != NULL) {
        expected = "[ 1448403340.0, { \"extra\": \"Some more data\", \"nested_key\": { \"to_nest\": \"This is the data to nest\" } } ]";
        TEST_IF_EQUIVALENT_JSON(output, expected);
        free(output);
    }
    flb_stop(ctx);
    flb_destroy(ctx);
}

void flb_test_filter_nest_multi(void)
{
    int ret;
    int bytes;
    char *p, *output, *expected;
    flb_ctx_t *ctx;
    int in_ffd;
    int out_ffd;
    int filter_ffd;

    struct flb_lib_out_cb cb;
    cb.cb   = callback_test;
    cb.data = NULL;

    ctx = flb_create();

    in_ffd = flb_input(ctx, (char *) "lib", NULL);
    TEST_CHECK(in_ffd >= 0);
    flb_input_set(ctx, in_ffd, "tag", "test", NULL);

    out_ffd = flb_output(ctx, (char *) "lib", &cb);
    TEST_CHECK(out_ffd >= 0);
    flb_output_set(ctx, out_ffd,
        "format", "json",
        "match", "test",
        NULL);
    filter_ffd = flb_filter(ctx, (char *) "nest", NULL);
    TEST_CHECK(filter_ffd >= 0);

    ret = flb_filter_set(ctx, filter_ffd,
        "Match", "*",
        "Operation", "nest",
        "Wildcard", "to_nest.*",
        "Nest_under", "nested_key",
        NULL);

    TEST_CHECK(ret == 0);

    ret = flb_start(ctx);
    TEST_CHECK(ret == 0);

    p = "[ 1448403340, { \"to_nest.test1\":\"This is the data to nest\", \"to_nest.test2\":\"This is also data to nest\", \"extra\":\"Some more data\" } ]";
    bytes = flb_lib_push(ctx, in_ffd, p, strlen(p));
    TEST_CHECK(bytes == strlen(p));

    /* waiting flush */
    do {
        output = get_output();
        sleep(1);
    } while (!output);

    TEST_CHECK_(output != NULL, "Expected output to not be NULL");

    if (output != NULL) {
        expected = "[ 1448403340.0, { \"extra\": \"Some more data\", \"nested_key\": {\"to_nest.test1\":\"This is the data to nest\", \"to_nest.test2\": \"This is also data to nest\" } } ]";
        TEST_IF_EQUIVALENT_JSON(output, expected);
        free(output);
    }
    flb_stop(ctx);
    flb_destroy(ctx);
}

void flb_test_filter_lift(void)
{
    int ret;
    int bytes;
    char *p, *output, *expected;
    flb_ctx_t *ctx;
    int in_ffd;
    int out_ffd;
    int filter_ffd;

    struct flb_lib_out_cb cb;
    cb.cb   = callback_test;
    cb.data = NULL;

    ctx = flb_create();

    in_ffd = flb_input(ctx, (char *) "lib", NULL);
    TEST_CHECK(in_ffd >= 0);
    flb_input_set(ctx, in_ffd, "tag", "test", NULL);

    out_ffd = flb_output(ctx, (char *) "lib", &cb);
    TEST_CHECK(out_ffd >= 0);
    flb_output_set(ctx, out_ffd,
        "format", "json",
        "match", "test",
        NULL);
    filter_ffd = flb_filter(ctx, (char *) "nest", NULL);
    TEST_CHECK(filter_ffd >= 0);

    ret = flb_filter_set(ctx, filter_ffd,
        "Match", "*",
        "Operation", "lift",
        "Nested_under", "to_lift",
        NULL);

    TEST_CHECK(ret == 0);

    ret = flb_start(ctx);
    TEST_CHECK(ret == 0);

    p = "[ 1448403340, { \"top-level\": \"Top-level key\", \"to_lift\": { \"test1\":\"This is the data to lift\", \"test2\": \"This is also data to lift\" } } ]";
    bytes = flb_lib_push(ctx, in_ffd, p, strlen(p));
    TEST_CHECK(bytes == strlen(p));

    /* waiting flush */
    do {
        output = get_output();
        sleep(1);
    } while (!output);

    TEST_CHECK_(output != NULL, "Expected output to not be NULL");

    if (output != NULL) {
        expected = "[ 1448403340.0, { \"top-level\": \"Top-level key\", \"test1\": \"This is the data to lift\", \"test2\": \"This is also data to lift\" } ]";
        TEST_IF_EQUIVALENT_JSON(output, expected);
        free(output);
    }
    flb_stop(ctx);
    flb_destroy(ctx);
}

void flb_test_filter_lift_add_prefix(void)
{
    int ret;
    int bytes;
    char *p, *output, *expected;
    flb_ctx_t *ctx;
    int in_ffd;
    int out_ffd;
    int filter_ffd;

    struct flb_lib_out_cb cb;
    cb.cb   = callback_test;
    cb.data = NULL;

    ctx = flb_create();

    in_ffd = flb_input(ctx, (char *) "lib", NULL);
    TEST_CHECK(in_ffd >= 0);
    flb_input_set(ctx, in_ffd, "tag", "test", NULL);

    out_ffd = flb_output(ctx, (char *) "lib", &cb);
    TEST_CHECK(out_ffd >= 0);
    flb_output_set(ctx, out_ffd,
        "format", "json",
        "match", "test",
        NULL);
    filter_ffd = flb_filter(ctx, (char *) "nest", NULL);
    TEST_CHECK(filter_ffd >= 0);

    ret = flb_filter_set(ctx, filter_ffd,
        "Match", "*",
        "Operation", "lift",
        "Nested_under", "to_lift",
        "Add_prefix", "foo.",
        NULL);

    TEST_CHECK(ret == 0);

    ret = flb_start(ctx);
    TEST_CHECK(ret == 0);

    p = "[ 1448403340, { \"top-level\": \"Top-level key\", \"to_lift\": { \"test1\": \"This is the data to lift\", \"test2\": \"This is also data to lift\" } } ]";
    bytes = flb_lib_push(ctx, in_ffd, p, strlen(p));
    TEST_CHECK(bytes == strlen(p));

    /* waiting flush */
    do {
        output = get_output();
        sleep(1);
    } while (!output);

    TEST_CHECK_(output != NULL, "Expected output to not be NULL");

    if (output != NULL) {
        expected = "[ 1448403340.0, { \"top-level\": \"Top-level key\", \"foo.test1\": \"This is the data to lift\", \"foo.test2\": \"This is also data to lift\" } ]";
        TEST_IF_EQUIVALENT_JSON(output, expected);
        free(output);
    }
    flb_stop(ctx);
    flb_destroy(ctx);
}
