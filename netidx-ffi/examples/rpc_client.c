/* RPC client: calls /example/add with two numbers from argv */
#include <netidx/netidx.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <a> <b>\n", argv[0]);
        return 1;
    }

    int64_t a = atoll(argv[1]);
    int64_t b = atoll(argv[2]);

    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxSubscriber *sub = NULL;
    NetidxRpcClient *client = NULL;

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
        const char *path_str = "/example/add";
        NetidxPath *path = netidx_path_new(path_str, strlen(path_str));
        client = netidx_rpc_client_new(sub, path, &err);
        netidx_path_destroy(path); /* borrowed */
        if (!client) goto fail;
    }

    {
        NetidxRpcArg args[2] = {
            { .name = "a", .name_len = 1, .value = netidx_value_i64(a) },
            { .name = "b", .name_len = 1, .value = netidx_value_i64(b) },
        };

        NetidxValue *result = netidx_rpc_client_call(rt, client, args, 2, 10000, &err);
        if (!result) goto fail;

        char *s = NULL;
        size_t slen = 0;
        netidx_value_to_string(result, &s, &slen);
        printf("%ld + %ld = %s\n", (long)a, (long)b, s);
        netidx_str_free(s);
        netidx_value_destroy(result);
    }

    netidx_rpc_client_destroy(client);
    netidx_subscriber_destroy(sub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    if (client) netidx_rpc_client_destroy(client);
    if (sub) netidx_subscriber_destroy(sub);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    return 1;
}
