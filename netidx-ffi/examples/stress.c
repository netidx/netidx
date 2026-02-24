/* Stress test: publish or subscribe 1000 values.
 *   stress_c publish   — publishes 1000 paths, updates in tight loop
 *   stress_c subscribe — subscribes to all 1000, counts updates/sec
 */
#include <netidx/netidx.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define N_PATHS 1000

static double now_secs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int run_publish(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = netidx_runtime_new(0, &err);
    if (!rt) { fprintf(stderr, "runtime: %s\n", netidx_error_message(&err)); return 1; }

    NetidxConfig *cfg = netidx_config_load_default(&err);
    if (!cfg) { fprintf(stderr, "config: %s\n", netidx_error_message(&err)); return 1; }

    NetidxPublisherBuilder *pb = netidx_publisher_builder_new(cfg);
    const char *bind = "local";
    netidx_publisher_builder_bind_cfg(pb, bind, strlen(bind), &err);
    NetidxPublisher *pub = netidx_publisher_builder_build(rt, pb, &err);
    if (!pub) { fprintf(stderr, "publisher: %s\n", netidx_error_message(&err)); return 1; }

    NetidxPublisherVal *vals[N_PATHS];
    const char *base_str = "/example/stress";
    NetidxPath *base = netidx_path_new(base_str, strlen(base_str));
    for (int i = 0; i < N_PATHS; i++) {
        char seg[16];
        int slen = snprintf(seg, sizeof(seg), "%d", i);
        NetidxPath *p = netidx_path_append(base, seg, (size_t)slen);
        vals[i] = netidx_publisher_publish(pub, p, netidx_value_i64(0), &err);
        /* p and init consumed */
        if (!vals[i]) { fprintf(stderr, "publish: %s\n", netidx_error_message(&err)); return 1; }
    }
    netidx_path_destroy(base);

    netidx_publisher_flushed(rt, pub);
    printf("published %d paths, starting updates\n", N_PATHS);

    for (int64_t round = 0;; round++) {
        NetidxUpdateBatch *batch = netidx_publisher_start_batch(pub);
        for (int i = 0; i < N_PATHS; i++)
            netidx_publisher_val_update(vals[i], batch, netidx_value_i64(round));
        netidx_update_batch_commit(rt, batch, -1);
        if (round % 100 == 0)
            printf("round %ld\n", (long)round);
    }

    /* unreachable cleanup */
    for (int i = 0; i < N_PATHS; i++)
        netidx_publisher_val_destroy(vals[i]);
    netidx_publisher_destroy(pub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;
}

static int run_subscribe(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = netidx_runtime_new(0, &err);
    if (!rt) { fprintf(stderr, "runtime: %s\n", netidx_error_message(&err)); return 1; }

    NetidxConfig *cfg = netidx_config_load_default(&err);
    if (!cfg) { fprintf(stderr, "config: %s\n", netidx_error_message(&err)); return 1; }

    NetidxSubscriberBuilder *sb = netidx_subscriber_builder_new(cfg);
    NetidxSubscriber *sub = netidx_subscriber_builder_build(rt, sb, &err);
    if (!sub) { fprintf(stderr, "subscriber: %s\n", netidx_error_message(&err)); return 1; }

    NetidxDval *dvals[N_PATHS];
    NetidxUpdateReceiver *rx = NULL;
    NetidxUpdateChannel *ch = netidx_update_channel_new(0, &rx);
    const char *base_str = "/example/stress";
    NetidxPath *base = netidx_path_new(base_str, strlen(base_str));
    for (int i = 0; i < N_PATHS; i++) {
        char seg[16];
        int slen = snprintf(seg, sizeof(seg), "%d", i);
        NetidxPath *p = netidx_path_append(base, seg, (size_t)slen);
        dvals[i] = netidx_subscriber_subscribe_updates(sub, p, ch, 0);
        netidx_path_destroy(p);
    }
    netidx_path_destroy(base);
    netidx_update_channel_destroy(ch); /* done subscribing, rx keeps channel alive */

    printf("subscribed to %d paths, counting updates\n", N_PATHS);

    uint64_t total = 0;
    double last = now_secs();

    for (;;) {
        size_t len = 0;
        NetidxSubscriberUpdate **updates =
            netidx_update_receiver_recv(rt, rx, 1000, &len);
        if (updates) {
            for (size_t j = 0; j < len; j++)
                netidx_subscriber_update_destroy(updates[j]);
            netidx_subscriber_update_array_free(updates, len);
            total += len;
        }
        double now = now_secs();
        if (now - last >= 1.0) {
            printf("updates/sec: %lu\n", (unsigned long)total);
            total = 0;
            last = now;
        }
    }

    /* unreachable cleanup */
    netidx_update_receiver_destroy(rx);
    for (int i = 0; i < N_PATHS; i++)
        netidx_dval_destroy(dvals[i]);
    netidx_subscriber_destroy(sub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2 || (strcmp(argv[1], "publish") != 0 && strcmp(argv[1], "subscribe") != 0)) {
        fprintf(stderr, "usage: %s publish|subscribe\n", argv[0]);
        return 1;
    }
    if (strcmp(argv[1], "publish") == 0)
        return run_publish();
    return run_subscribe();
}
