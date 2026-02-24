/* Subscribe to one or more paths and print every update */
#include <netidx/netidx.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <path> [path...]\n", argv[0]);
        return 1;
    }
    int n_paths = argc - 1;

    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxSubscriber *sub = NULL;
    NetidxDval **dvals = NULL;
    NetidxUpdateChannel *ch = NULL;
    NetidxUpdateReceiver *rx = NULL;

    rt = netidx_runtime_new(0, &err);
    if (!rt) goto fail;

    cfg = netidx_config_load_default(&err);
    if (!cfg) goto fail;

    {
        NetidxSubscriberBuilder *sb = netidx_subscriber_builder_new(cfg);
        sub = netidx_subscriber_builder_build(rt, sb, &err);
        /* sb consumed */
        if (!sub) goto fail;
    }

    dvals = calloc((size_t)n_paths, sizeof(*dvals));
    if (!dvals) {
        fprintf(stderr, "out of memory\n");
        goto cleanup;
    }

    ch = netidx_update_channel_new(0, &rx);

    for (int i = 0; i < n_paths; i++) {
        NetidxPath *path = netidx_path_new(argv[i + 1], strlen(argv[i + 1]));
        dvals[i] = netidx_subscriber_subscribe_updates(sub, path, ch, 0);
        netidx_path_destroy(path);
    }
    netidx_update_channel_destroy(ch);
    ch = NULL;

    for (int i = 0; i < n_paths; i++) {
        if (!netidx_dval_wait_subscribed(rt, dvals[i], 10000, &err)) {
            fprintf(stderr, "warning: %s: %s\n", argv[i + 1], netidx_error_message(&err));
            netidx_error_free(&err);
            err = netidx_error_init();
        }
    }

    printf("subscribed to %d path(s)\n", n_paths);

    for (;;) {
        size_t len = 0;
        NetidxSubscriberUpdate **updates =
            netidx_update_receiver_recv(rt, rx, 1000, &len);
        if (!updates) continue;
        for (size_t j = 0; j < len; j++) {
            NetidxValue *v = netidx_subscriber_update_value_clone(updates[j]);
            if (v) {
                char *s = NULL;
                size_t slen = 0;
                netidx_value_to_string(v, &s, &slen);
                uint64_t sid = netidx_subscriber_update_sub_id(updates[j]);
                if (n_paths > 1)
                    printf("[%lu] %s\n", (unsigned long)sid, s);
                else
                    printf("%s\n", s);
                netidx_str_free(s);
                netidx_value_destroy(v);
            }
            netidx_subscriber_update_destroy(updates[j]);
        }
        netidx_subscriber_update_array_free(updates, len);
    }

cleanup:
    if (rx) netidx_update_receiver_destroy(rx);
    if (ch) netidx_update_channel_destroy(ch);
    for (int i = 0; i < n_paths; i++) {
        if (dvals && dvals[i]) netidx_dval_destroy(dvals[i]);
    }
    free(dvals);
    if (sub) netidx_subscriber_destroy(sub);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    goto cleanup;
}
