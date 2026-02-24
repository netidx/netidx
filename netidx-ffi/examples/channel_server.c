/* Channel echo server at /example/echo */
#include <netidx/netidx.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxPublisher *pub = NULL;
    NetidxChannelListener *listener = NULL;

    rt = netidx_runtime_new(0, &err);
    if (!rt) goto fail;

    cfg = netidx_config_load_default(&err);
    if (!cfg) goto fail;

    {
        NetidxPublisherBuilder *pb = netidx_publisher_builder_new(cfg);
        const char *bind = "local";
        netidx_publisher_builder_bind_cfg(pb, bind, strlen(bind), &err);
        pub = netidx_publisher_builder_build(rt, pb, &err);
        if (!pub) goto fail;
    }

    {
        const char *path_str = "/example/echo";
        NetidxPath *path = netidx_path_new(path_str, strlen(path_str));
        listener = netidx_channel_listener_new(rt, pub, path, -1, &err);
        netidx_path_destroy(path); /* borrowed */
        if (!listener) goto fail;
    }

    netidx_publisher_flushed(rt, pub);
    printf("echo server listening at /example/echo\n");

    for (;;) {
        NetidxChannelServerConn *conn =
            netidx_channel_listener_accept(rt, listener, -1, &err);
        if (!conn) {
            fprintf(stderr, "accept: %s\n", netidx_error_message(&err));
            netidx_error_free(&err);
            err = netidx_error_init();
            continue;
        }
        printf("client connected\n");

        while (!netidx_channel_server_conn_is_dead(conn)) {
            NetidxValue *v = netidx_channel_server_conn_recv_one(rt, conn, 1000, &err);
            if (!v) {
                /* timeout or error — check if dead */
                netidx_error_free(&err);
                err = netidx_error_init();
                continue;
            }
            char *s = NULL;
            size_t slen = 0;
            netidx_value_to_string(v, &s, &slen);
            printf("echo: %s\n", s);
            netidx_str_free(s);

            if (!netidx_channel_server_conn_send_one(rt, conn, v, &err)) {
                /* v consumed even on failure */
                fprintf(stderr, "send: %s\n", netidx_error_message(&err));
                netidx_error_free(&err);
                err = netidx_error_init();
                break;
            }
            /* v consumed by send_one */
        }

        printf("client disconnected\n");
        netidx_channel_server_conn_destroy(conn);
    }

    netidx_channel_listener_destroy(listener);
    netidx_publisher_destroy(pub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    if (listener) netidx_channel_listener_destroy(listener);
    if (pub) netidx_publisher_destroy(pub);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    return 1;
}
