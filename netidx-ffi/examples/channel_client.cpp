// Channel client: sends messages to /example/echo and prints replies
#include <netidx/netidx.hpp>
#include <cstdio>
#include <string_view>

int main() {
    try {
        netidx::Runtime rt;
        auto cfg = netidx::Config::load_default();
        auto sub = netidx::SubscriberBuilder(cfg).build(rt);

        netidx::Path path("/example/echo");
        auto conn = netidx::channel::ClientConnection::connect(rt, sub, path);
        printf("connected to /example/echo\n");

        std::string_view messages[] = {"hello", "world", "foo", "bar"};
        for (auto msg : messages) {
            conn.send(netidx::Value(msg));
            conn.flush(rt);
            auto reply = conn.recv_one(rt, 10000);
            printf("sent \"%.*s\", got back: %s\n",
                   (int)msg.size(), msg.data(), reply.to_string().c_str());
        }
    } catch (const netidx::Error& e) {
        fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
