// Publish an incrementing counter at /example/counter
#include <netidx/netidx.hpp>
#include <cstdio>
#include <unistd.h>

int main() {
    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto pub = netidx::PublisherBuilder(cfg)
            .bind_cfg("local")
            .build(rt);

        netidx::Path path("/example/counter");
        auto val = pub.publish(std::move(path), netidx::Value(int64_t(0)));
        pub.flushed(rt);
        printf("published /example/counter, starting updates\n");

        for (int64_t i = 1;; i++) {
            sleep(1);
            auto batch = pub.start_batch();
            val.update(batch, netidx::Value(i));
            batch.commit(rt);
            printf("updated: %ld\n", (long)i);
        }
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
