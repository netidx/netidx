// Subscribe to one or more paths and print every update
#include <netidx/netidx.hpp>
#include <cstdio>
#include <string>
#include <vector>
#include <unistd.h>

int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <path> [path...]\n", argv[0]);
        return 1;
    }
    int n_paths = argc - 1;

    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto sub = netidx::SubscriberBuilder(cfg).build(rt);

        auto [ch, rx] = netidx::UpdateChannel::create();
        std::vector<netidx::Dval> dvals;
        dvals.reserve(n_paths);

        for (int i = 0; i < n_paths; i++) {
            netidx::Path path(argv[i + 1]);
            dvals.push_back(sub.subscribe_updates(path, ch));
        }

        for (int i = 0; i < n_paths; i++) {
            try {
                dvals[i].wait_subscribed(rt, 10000);
            } catch (const netidx::Error& e) {
                fprintf(stderr, "warning: %s: %s\n", argv[i + 1], e.what());
            }
        }

        printf("subscribed to %d path(s)\n", n_paths);

        for (;;) {
            auto updates = rx.recv(rt, 1000);
            for (auto& u : updates) {
                if (u.event_type() == NETIDX_EVENT_TYPE_UPDATE) {
                    auto v = u.value_clone();
                    printf("%s\n", v.to_string().c_str());
                }
            }
        }
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
