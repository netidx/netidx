/* RPC server: defines "add" at /example/add that sums two i64 arguments */
#include <netidx/netidx.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void add_handler(void *userdata, NetidxRpcCall *call) {
    (void)userdata;

    NetidxValue *va = netidx_rpc_call_take_arg(call, "a", 1);
    NetidxValue *vb = netidx_rpc_call_take_arg(call, "b", 1);

    int64_t a = 0, b = 0;
    if (!va || !netidx_value_get_i64(va, &a) ||
        !vb || !netidx_value_get_i64(vb, &b)) {
        const char *msg = "arguments must be i64";
        NetidxValue *errmsg = netidx_value_string(msg, strlen(msg));
        netidx_rpc_call_reply(call, netidx_value_error(errmsg));
    } else {
        printf("add(%ld, %ld) = %ld\n", (long)a, (long)b, (long)(a + b));
        netidx_rpc_call_reply(call, netidx_value_i64(a + b));
    }

    if (va) netidx_value_destroy(va);
    if (vb) netidx_value_destroy(vb);
}

int main(void) {
    NetidxError err = netidx_error_init();
    NetidxRuntime *rt = NULL;
    NetidxConfig *cfg = NULL;
    NetidxPublisher *pub = NULL;
    NetidxRpcProc *proc = NULL;

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
        const char *path_str = "/example/add";
        NetidxPath *path = netidx_path_new(path_str, strlen(path_str));
        const char *doc_str = "add two integers";
        NetidxValue *doc = netidx_value_string(doc_str, strlen(doc_str));

        NetidxArgSpec args[2] = {
            { .name = "a", .name_len = 1,
              .doc = netidx_value_string("first operand", 13),
              .default_value = netidx_value_i64(0) },
            { .name = "b", .name_len = 1,
              .doc = netidx_value_string("second operand", 14),
              .default_value = netidx_value_i64(0) },
        };

        proc = netidx_rpc_proc_new(pub, path, doc, args, 2, add_handler, NULL, &err);
        /* path, doc, and arg values consumed */
        if (!proc) goto fail;
    }

    netidx_publisher_flushed(rt, pub);
    printf("RPC server running at /example/add (Ctrl-C to stop)\n");

    for (;;) sleep(3600);

    netidx_rpc_proc_destroy(proc);
    netidx_publisher_destroy(pub);
    netidx_config_destroy(cfg);
    netidx_runtime_destroy(rt);
    netidx_error_free(&err);
    return 0;

fail:
    fprintf(stderr, "error: %s\n", netidx_error_message(&err));
    netidx_error_free(&err);
    if (proc) netidx_rpc_proc_destroy(proc);
    if (pub) netidx_publisher_destroy(pub);
    if (cfg) netidx_config_destroy(cfg);
    if (rt) netidx_runtime_destroy(rt);
    return 1;
}
