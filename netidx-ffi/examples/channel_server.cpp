// Channel echo server at /example/echo
#include <netidx/netidx.hpp>
#include <cstdio>

int main() {
    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto pub = netidx::PublisherBuilder(cfg).bind_cfg("local").build(rt);

        netidx::Path path("/example/echo");
        netidx::channel::Listener listener(rt, pub, path);
        pub.flushed(rt);
        printf("echo server listening at /example/echo\n");

        for (;;) {
            auto conn = listener.accept(rt);
            printf("client connected\n");

            while (!conn.is_dead()) {
                try {
                    auto v = conn.recv_one(rt, 1000);
                    printf("echo: %s\n", v.to_string().c_str());
                    conn.send_one(rt, std::move(v));
                } catch (const netidx::Error&) {
                    /* timeout or recv error — check is_dead on next iteration */
                }
            }

            printf("client disconnected\n");
        }
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
