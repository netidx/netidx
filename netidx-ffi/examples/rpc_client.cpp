// RPC client: calls /example/add with two numbers from argv
#include <netidx/netidx.hpp>
#include <cstdio>
#include <cstdlib>

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <a> <b>\n", argv[0]);
        return 1;
    }

    int64_t a = atoll(argv[1]);
    int64_t b = atoll(argv[2]);

    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto sub = netidx::SubscriberBuilder(cfg).build(rt);

        netidx::Path path("/example/add");
        netidx::rpc::Client client(sub, path);

        netidx::rpc::Client::Arg args[] = {
            { "a", netidx::Value(a) },
            { "b", netidx::Value(b) },
        };

        auto result = client.call(rt, args, 10000);
        printf("%ld + %ld = %s\n", (long)a, (long)b, result.to_string().c_str());
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
