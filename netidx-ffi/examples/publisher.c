/* Publish an incrementing counter at /example/counter */
#include <netidx/netidx.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxPublisherBuilder *pb = NULL;
    NetidxPublisher *pub = NULL;
    NetidxPublisherVal *val = NULL;

    rt = netidx_runtime_new(0, &err);
    if (!rt) goto fail;

    cfg = netidx_config_load_default(&err);
    if (!cfg) goto fail;

    pb = netidx_publisher_builder_new(cfg);
    {
        const char *bind = "local";
        if (!netidx_publisher_builder_bind_cfg(pb, bind, strlen(bind), &err))
            goto fail;
    }

    pub = netidx_publisher_builder_build(rt, pb, &err);
    pb = NULL; /* consumed */
    if (!pub) goto fail;

    {
        const char *path_str = "/example/counter";
        NetidxPath *path = netidx_path_new(path_str, strlen(path_str));
        val = netidx_publisher_publish(pub, path, netidx_value_i64(0), &err);
        /* path and init value consumed by publish */
        if (!val) goto fail;
    }

    netidx_publisher_flushed(rt, pub);
    printf("published /example/counter, starting updates\n");

    for (int64_t i = 1;; i++) {
        sleep(1);
        NetidxUpdateBatch *batch = netidx_publisher_start_batch(pub);
        netidx_publisher_val_update(val, batch, netidx_value_i64(i));
        netidx_update_batch_commit(rt, batch, -1);
        printf("updated: %ld\n", (long)i);
    }

    /* unreachable, but for completeness */
    netidx_publisher_val_destroy(val);
    netidx_publisher_destroy(pub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    if (val) netidx_publisher_val_destroy(val);
    if (pub) netidx_publisher_destroy(pub);
    if (pb) netidx_publisher_builder_destroy(pb);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    return 1;
}
