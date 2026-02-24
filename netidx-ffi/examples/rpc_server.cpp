// RPC server: defines "add" at /example/add that sums two i64 arguments
#include <netidx/netidx.hpp>
#include <cstdio>
#include <unistd.h>

int main() {
    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto pub = netidx::PublisherBuilder(cfg).bind_cfg("local").build(rt);

        netidx::rpc::Proc::ArgSpec args[] = {
            { "a", netidx::Value("first operand"),  netidx::Value(int64_t(0)) },
            { "b", netidx::Value("second operand"), netidx::Value(int64_t(0)) },
        };

        auto proc = netidx::rpc::Proc::create(
            pub,
            netidx::Path("/example/add"),
            netidx::Value("add two integers"),
            args,
            [](netidx::rpc::Call call) {
                auto va = call.take_arg("a");
                auto vb = call.take_arg("b");
                auto a = va.as_i64();
                auto b = vb.as_i64();
                if (!a || !b) {
                    call.reply(netidx::Value::error(
                        netidx::Value("arguments must be i64")));
                } else {
                    printf("add(%ld, %ld) = %ld\n", (long)*a, (long)*b, (long)(*a + *b));
                    call.reply(netidx::Value(int64_t(*a + *b)));
                }
            }
        );

        pub.flushed(rt);
        printf("RPC server running at /example/add (Ctrl-C to stop)\n");

        for (;;) sleep(3600);
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
