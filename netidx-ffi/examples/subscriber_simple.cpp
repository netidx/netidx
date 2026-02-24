// Subscribe to a single path and print every update
#include <netidx/netidx.hpp>
#include <cstdio>

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <path>\n", argv[0]);
        return 1;
    }

    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto sub = netidx::SubscriberBuilder(cfg).build(rt);

        auto [ch, rx] = netidx::UpdateChannel::create();
        netidx::Path path(argv[1]);
        auto dval = sub.subscribe_updates(path, ch);
        dval.wait_subscribed(rt, 10000);

        printf("subscribed to %s\n", argv[1]);

        for (;;) {
            for (auto& u : rx.recv(rt)) {
                if (u.event_type() == NETIDX_EVENT_TYPE_UPDATE)
                    printf("%s\n", u.value_clone().to_string().c_str());
            }
        }
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
