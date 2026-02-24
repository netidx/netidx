// Stress test: publish or subscribe 1000 values
//   stress_cpp publish   — publishes 1000 paths, updates in tight loop
//   stress_cpp subscribe — subscribes to all 1000, counts updates/sec
#include <netidx/netidx.hpp>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>
#include <vector>
#include <unistd.h>

static constexpr int N_PATHS = 1000;

static double now_secs() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return double(ts.tv_sec) + double(ts.tv_nsec) / 1e9;
}

static int run_publish() {
    netidx::Runtime rt;
    auto cfg = netidx::Config::load_default();
    auto pub = netidx::PublisherBuilder(cfg).bind_cfg("local").build(rt);

    netidx::Path base("/example/stress");
    std::vector<netidx::PublisherVal> vals;
    vals.reserve(N_PATHS);
    for (int i = 0; i < N_PATHS; i++)
        vals.push_back(pub.publish(base.append(std::to_string(i)),
                                   netidx::Value(int64_t(0))));

    pub.flushed(rt);
    printf("published %d paths, starting updates\n", N_PATHS);

    for (int64_t round = 0;; round++) {
        auto batch = pub.start_batch();
        for (auto& v : vals)
            v.update(batch, netidx::Value(round));
        batch.commit(rt);
        if (round % 100 == 0)
            printf("round %ld\n", (long)round);
    }
    return 0;
}

static int run_subscribe() {
    netidx::Runtime rt;
    auto cfg = netidx::Config::load_default();
    auto sub = netidx::SubscriberBuilder(cfg).build(rt);

    netidx::Path base("/example/stress");
    auto [ch, rx] = netidx::UpdateChannel::create();
    std::vector<netidx::Dval> dvals;
    dvals.reserve(N_PATHS);
    for (int i = 0; i < N_PATHS; i++)
        dvals.push_back(sub.subscribe_updates(base.append(std::to_string(i)), ch));

    printf("subscribed to %d paths, counting updates\n", N_PATHS);

    uint64_t total = 0;
    double last = now_secs();

    for (;;) {
        auto updates = rx.recv(rt, 1000);
        total += updates.size();
        double now = now_secs();
        if (now - last >= 1.0) {
            printf("updates/sec: %lu\n", (unsigned long)total);
            total = 0;
            last = now;
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2 || (strcmp(argv[1], "publish") != 0 && strcmp(argv[1], "subscribe") != 0)) {
        fprintf(stderr, "usage: %s publish|subscribe\n", argv[0]);
        return 1;
    }
    try {
        if (strcmp(argv[1], "publish") == 0)
            return run_publish();
        return run_subscribe();
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
