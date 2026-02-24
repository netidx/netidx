/* Subscribe to a single path and print every update */
#include <netidx/netidx.h>
#include <stdio.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <path>\n", argv[0]);
        return 1;
    }

    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = netidx_runtime_new(0, &err);
    if (!rt) goto fail;

    NetidxConfig *cfg = netidx_config_load_default(&err);
    if (!cfg) goto fail;

    NetidxSubscriber *sub;
    {
        NetidxSubscriberBuilder *sb = netidx_subscriber_builder_new(cfg);
        sub = netidx_subscriber_builder_build(rt, sb, &err);
        if (!sub) goto fail;
    }

    NetidxUpdateReceiver *rx;
    NetidxUpdateChannel *ch = netidx_update_channel_new(0, &rx);

    NetidxPath *path = netidx_path_new(argv[1], strlen(argv[1]));
    NetidxDval *dval = netidx_subscriber_subscribe_updates(sub, path, ch, 0);
    netidx_path_destroy(path);
    netidx_update_channel_destroy(ch);

    if (!netidx_dval_wait_subscribed(rt, dval, 10000, &err)) goto fail;

    printf("subscribed to %s\n", argv[1]);

    for (;;) {
        size_t len = 0;
        NetidxSubscriberUpdate **updates =
            netidx_update_receiver_recv(rt, rx, -1, &len);
        if (!updates) continue;
        for (size_t i = 0; i < len; i++) {
            NetidxValue *v = netidx_subscriber_update_value_clone(updates[i]);
            if (v) {
                char *s = NULL;
                size_t slen = 0;
                netidx_value_to_string(v, &s, &slen);
                printf("%s\n", s);
                netidx_str_free(s);
                netidx_value_destroy(v);
            }
            netidx_subscriber_update_destroy(updates[i]);
        }
        netidx_subscriber_update_array_free(updates, len);
    }

    netidx_dval_destroy(dval);
    netidx_update_receiver_destroy(rx);
    netidx_subscriber_destroy(sub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    return 1;
}
