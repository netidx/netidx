/* Channel client: sends messages to /example/echo and prints replies */
#include <netidx/netidx.h>
#include <stdio.h>
#include <string.h>

int main(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxSubscriber *sub = NULL;
    NetidxChannelClientConn *conn = NULL;

    rt = netidx_runtime_new(0, &err);
    if (!rt) goto fail;

    cfg = netidx_config_load_default(&err);
    if (!cfg) goto fail;

    {
        NetidxSubscriberBuilder *sb = netidx_subscriber_builder_new(cfg);
        sub = netidx_subscriber_builder_build(rt, sb, &err);
        if (!sub) goto fail;
    }

    {
        const char *path_str = "/example/echo";
        NetidxPath *path = netidx_path_new(path_str, strlen(path_str));
        conn = netidx_channel_client_connect(rt, sub, path, &err);
        netidx_path_destroy(path);
        if (!conn) goto fail;
    }

    printf("connected to /example/echo\n");

    const char *messages[] = {"hello", "world", "foo", "bar"};
    for (int i = 0; i < 4; i++) {
        const char *msg = messages[i];
        if (!netidx_channel_client_conn_send(conn, netidx_value_string(msg, strlen(msg)), &err))
            goto fail;
        if (!netidx_channel_client_conn_flush(rt, conn, &err))
            goto fail;

        NetidxValue *reply = netidx_channel_client_conn_recv_one(rt, conn, 10000, &err);
        if (!reply) goto fail;

        char *s = NULL;
        size_t slen = 0;
        netidx_value_to_string(reply, &s, &slen);
        printf("sent \"%s\", got back: %s\n", msg, s);
        netidx_str_free(s);
        netidx_value_destroy(reply);
    }

    netidx_channel_client_conn_destroy(conn);
    netidx_subscriber_destroy(sub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    if (conn) netidx_channel_client_conn_destroy(conn);
    if (sub) netidx_subscriber_destroy(sub);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    return 1;
}
