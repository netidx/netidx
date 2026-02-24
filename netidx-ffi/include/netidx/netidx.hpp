// Idiomatic C++ wrapper for netidx FFI
// Zero-overhead RAII wrappers over the C API (netidx.h)
#pragma once

#include "netidx.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace netidx {

// --- Error ---

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

namespace detail {

inline void check_error(NetidxError& err) {
    const char* msg = netidx_error_message(&err);
    if (msg) {
        std::string s(msg);
        netidx_error_free(&err);
        throw Error(std::move(s));
    }
}

} // namespace detail

// --- Forward declarations ---

class Runtime;
class Config;
class Path;
class Value;
class Publisher;
class PublisherBuilder;
class PublisherVal;
class UpdateBatch;
class WriteRequest;
class Subscriber;
class SubscriberBuilder;
class Dval;
class Event;
class SubscriberUpdate;

namespace rpc { class Call; class Client; }
namespace channel { class ServerConnection; class ClientConnection; }

using ValueType = NetidxValueType;
using EventType = NetidxEventType;

// --- Runtime ---

class Runtime {
    NetidxRuntime* ptr_;
public:
    explicit Runtime(size_t worker_threads = 0) {
        NetidxError err = netidx_error_init();
        ptr_ = netidx_runtime_new(worker_threads, &err);
        detail::check_error(err);
    }
    ~Runtime() { if (ptr_) netidx_runtime_destroy(ptr_); }

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    Runtime(Runtime&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Runtime& operator=(Runtime&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_runtime_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxRuntime* raw() const { return ptr_; }
};

// --- Config ---

class Config {
    NetidxConfig* ptr_;
public:
    static Config load_default() {
        NetidxError err = netidx_error_init();
        auto* p = netidx_config_load_default(&err);
        detail::check_error(err);
        return Config(p);
    }

    static Config load(std::string_view path) {
        NetidxError err = netidx_error_init();
        auto* p = netidx_config_load(path.data(), path.size(), &err);
        detail::check_error(err);
        return Config(p);
    }

    ~Config() { if (ptr_) netidx_config_destroy(ptr_); }

    Config(const Config& o) : ptr_(netidx_config_clone(o.ptr_)) {}
    Config& operator=(const Config& o) {
        if (this != &o) { if (ptr_) netidx_config_destroy(ptr_); ptr_ = netidx_config_clone(o.ptr_); }
        return *this;
    }
    Config(Config&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Config& operator=(Config&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_config_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxConfig* raw() const { return ptr_; }

private:
    explicit Config(NetidxConfig* p) : ptr_(p) {}
};

// --- Path ---

class Path {
    NetidxPath* ptr_;
public:
    explicit Path(std::string_view s)
        : ptr_(netidx_path_new(s.data(), s.size())) {}

    Path append(std::string_view seg) const {
        return Path(netidx_path_append(ptr_, seg.data(), seg.size()));
    }

    std::string_view as_str() const {
        const char* data;
        size_t len;
        netidx_path_as_str(ptr_, &data, &len);
        return {data, len};
    }

    size_t levels() const { return netidx_path_levels(ptr_); }

    std::optional<std::string_view> basename() const {
        const char* data;
        size_t len;
        if (netidx_path_basename(ptr_, &data, &len))
            return std::string_view{data, len};
        return std::nullopt;
    }

    std::optional<std::string_view> dirname() const {
        const char* data;
        size_t len;
        if (netidx_path_dirname(ptr_, &data, &len))
            return std::string_view{data, len};
        return std::nullopt;
    }

    ~Path() { if (ptr_) netidx_path_destroy(ptr_); }

    Path(const Path& o) : ptr_(netidx_path_clone(o.ptr_)) {}
    Path& operator=(const Path& o) {
        if (this != &o) { if (ptr_) netidx_path_destroy(ptr_); ptr_ = netidx_path_clone(o.ptr_); }
        return *this;
    }
    Path(Path&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Path& operator=(Path&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_path_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxPath* raw() const { return ptr_; }
    // Consume the path, transferring ownership to the caller
    NetidxPath* release() { auto* p = ptr_; ptr_ = nullptr; return p; }

private:
    explicit Path(NetidxPath* p) : ptr_(p) {}
    friend class Publisher;
};

// --- Value ---

class Value {
    NetidxValue* ptr_;
public:
    // Null
    Value() : ptr_(netidx_value_null()) {}

    // Scalars
    explicit Value(uint8_t v) : ptr_(netidx_value_u8(v)) {}
    explicit Value(int8_t v) : ptr_(netidx_value_i8(v)) {}
    explicit Value(uint16_t v) : ptr_(netidx_value_u16(v)) {}
    explicit Value(int16_t v) : ptr_(netidx_value_i16(v)) {}
    explicit Value(uint32_t v) : ptr_(netidx_value_u32(v)) {}
    explicit Value(int32_t v) : ptr_(netidx_value_i32(v)) {}
    explicit Value(uint64_t v) : ptr_(netidx_value_u64(v)) {}
    explicit Value(int64_t v) : ptr_(netidx_value_i64(v)) {}
    explicit Value(float v) : ptr_(netidx_value_f32(v)) {}
    explicit Value(double v) : ptr_(netidx_value_f64(v)) {}
    explicit Value(bool v) : ptr_(netidx_value_bool(v)) {}

    // String
    explicit Value(std::string_view s)
        : ptr_(netidx_value_string(s.data(), s.size())) {}
    explicit Value(const char* s)
        : ptr_(netidx_value_string(s, std::char_traits<char>::length(s))) {}

    // Bytes
    explicit Value(std::span<const uint8_t> b)
        : ptr_(netidx_value_bytes(b.data(), b.size())) {}

    // Array (clones each element; originals remain valid)
    explicit Value(std::span<const Value> vals) {
        std::vector<NetidxValue*> ptrs;
        ptrs.reserve(vals.size());
        for (auto& v : vals)
            ptrs.push_back(netidx_value_clone(v.raw()));
        ptr_ = netidx_value_array(ptrs.data(), ptrs.size());
    }

    ValueType type() const { return netidx_value_type(ptr_); }

    // Scalar getters - return std::optional
    std::optional<uint8_t> as_u8() const { uint8_t v; return netidx_value_get_u8(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<int8_t> as_i8() const { int8_t v; return netidx_value_get_i8(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<uint16_t> as_u16() const { uint16_t v; return netidx_value_get_u16(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<int16_t> as_i16() const { int16_t v; return netidx_value_get_i16(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<uint32_t> as_u32() const { uint32_t v; return netidx_value_get_u32(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<int32_t> as_i32() const { int32_t v; return netidx_value_get_i32(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<uint64_t> as_u64() const { uint64_t v; return netidx_value_get_u64(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<int64_t> as_i64() const { int64_t v; return netidx_value_get_i64(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<float> as_f32() const { float v; return netidx_value_get_f32(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<double> as_f64() const { double v; return netidx_value_get_f64(ptr_, &v) ? std::optional(v) : std::nullopt; }
    std::optional<bool> as_bool() const { bool v; return netidx_value_get_bool(ptr_, &v) ? std::optional(v) : std::nullopt; }

    // String/bytes - borrow from the Value
    std::optional<std::string_view> as_string() const {
        const char* data; size_t len;
        if (netidx_value_get_string(ptr_, &data, &len))
            return std::string_view{data, len};
        return std::nullopt;
    }
    std::optional<std::span<const uint8_t>> as_bytes() const {
        const uint8_t* data; size_t len;
        if (netidx_value_get_bytes(ptr_, &data, &len))
            return std::span<const uint8_t>{data, len};
        return std::nullopt;
    }

    // Error inner value
    Value error_clone() const {
        auto* p = netidx_value_get_error_clone(ptr_);
        if (!p) throw Error("not an Error value");
        return Value(p);
    }

    // Array access
    size_t array_len() const { return netidx_value_array_len(ptr_); }
    Value array_get(size_t i) const {
        auto* p = netidx_value_array_get_clone(ptr_, i);
        if (!p) throw Error("array index out of bounds or not an array");
        return Value(p);
    }

    // Map
    static Value map(std::span<const Value> keys, std::span<const Value> values) {
        if (keys.size() != values.size())
            throw Error("map keys and values must have equal length");
        std::vector<NetidxValue*> k_ptrs, v_ptrs;
        k_ptrs.reserve(keys.size());
        v_ptrs.reserve(values.size());
        for (auto& k : keys)
            k_ptrs.push_back(netidx_value_clone(k.raw()));
        for (auto& v : values)
            v_ptrs.push_back(netidx_value_clone(v.raw()));
        return Value(netidx_value_map(k_ptrs.data(), v_ptrs.data(), keys.size()));
    }

    size_t map_len() const { return netidx_value_map_len(ptr_); }

    std::optional<Value> map_get(const Value& key) const {
        auto* p = netidx_value_map_get_clone(ptr_, key.raw());
        if (!p) return std::nullopt;
        return Value(p);
    }

    std::vector<std::pair<Value, Value>> map_entries() const {
        NetidxValue** keys = nullptr;
        NetidxValue** vals = nullptr;
        size_t len = 0;
        if (!netidx_value_map_entries_clone(ptr_, &keys, &vals, &len))
            return {};
        std::vector<std::pair<Value, Value>> result;
        result.reserve(len);
        for (size_t i = 0; i < len; ++i)
            result.emplace_back(Value(keys[i]), Value(vals[i]));
        netidx_value_array_free(keys, len);
        netidx_value_array_free(vals, len);
        return result;
    }

    // Decimal
    static Value decimal(std::string_view s) {
        NetidxError err = netidx_error_init();
        auto* p = netidx_value_decimal(s.data(), s.size(), &err);
        detail::check_error(err);
        return Value(p);
    }

    std::optional<std::string> as_decimal_string() const {
        char* data; size_t len;
        if (!netidx_value_get_decimal_string(ptr_, &data, &len))
            return std::nullopt;
        std::string s(data, len);
        netidx_str_free(data);
        return s;
    }

    // DateTime
    struct DateTimeComponents { int64_t secs; uint32_t nsecs; };

    static Value datetime(int64_t secs, uint32_t nsecs = 0) {
        return Value(netidx_value_datetime(secs, nsecs));
    }

    std::optional<DateTimeComponents> as_datetime() const {
        int64_t secs; uint32_t nsecs;
        if (!netidx_value_get_datetime(ptr_, &secs, &nsecs))
            return std::nullopt;
        return DateTimeComponents{secs, nsecs};
    }

    // Duration
    struct DurationComponents { uint64_t secs; uint32_t nsecs; };

    static Value duration(uint64_t secs, uint32_t nsecs = 0) {
        return Value(netidx_value_duration(secs, nsecs));
    }

    std::optional<DurationComponents> as_duration() const {
        uint64_t secs; uint32_t nsecs;
        if (!netidx_value_get_duration(ptr_, &secs, &nsecs))
            return std::nullopt;
        return DurationComponents{secs, nsecs};
    }

    // Abstract
    std::optional<std::array<uint8_t, 16>> abstract_id() const {
        std::array<uint8_t, 16> id;
        if (!netidx_value_abstract_id(ptr_, &id))
            return std::nullopt;
        return id;
    }

    std::optional<std::vector<uint8_t>> abstract_encode() const {
        uint8_t* data; size_t len;
        if (!netidx_value_abstract_encode(ptr_, &data, &len))
            return std::nullopt;
        std::vector<uint8_t> result(data, data + len);
        netidx_bytes_free(data, len);
        return result;
    }

    static Value abstract_decode(std::span<const uint8_t> data) {
        NetidxError err = netidx_error_init();
        auto* p = netidx_value_abstract_decode(data.data(), data.size(), &err);
        detail::check_error(err);
        return Value(p);
    }

    // Display
    std::string to_string() const {
        char* data; size_t len;
        netidx_value_to_string(ptr_, &data, &len);
        std::string s(data, len);
        netidx_str_free(data);
        return s;
    }

    ~Value() { if (ptr_) netidx_value_destroy(ptr_); }

    Value(const Value& o) : ptr_(netidx_value_clone(o.ptr_)) {}
    Value& operator=(const Value& o) {
        if (this != &o) { if (ptr_) netidx_value_destroy(ptr_); ptr_ = netidx_value_clone(o.ptr_); }
        return *this;
    }
    Value(Value&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Value& operator=(Value&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_value_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxValue* raw() const { return ptr_; }
    NetidxValue* release() { auto* p = ptr_; ptr_ = nullptr; return p; }

private:
    explicit Value(NetidxValue* p) : ptr_(p) {}
    friend class PublisherVal;
    friend class UpdateBatch;
    friend class Dval;
    friend class Event;
    friend class SubscriberUpdate;
    friend class WriteRequest;
    friend class rpc::Call;
    friend class rpc::Client;
    friend class channel::ServerConnection;
    friend class channel::ClientConnection;
};

// --- Event ---

class Event {
    NetidxEvent* ptr_;
public:
    EventType type() const { return netidx_event_type(ptr_); }

    Value value_clone() const {
        auto* p = netidx_event_value_clone(ptr_);
        if (!p) throw Error("event has no value (Unsubscribed)");
        return Value(p);
    }

    ~Event() { if (ptr_) netidx_event_destroy(ptr_); }

    Event(const Event&) = delete;
    Event& operator=(const Event&) = delete;
    Event(Event&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Event& operator=(Event&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_event_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit Event(NetidxEvent* p) : ptr_(p) {}
    friend class Dval;
};

// --- SubscriberUpdate ---

class SubscriberUpdate {
    NetidxSubscriberUpdate* ptr_;
public:
    uint64_t sub_id() const { return netidx_subscriber_update_sub_id(ptr_); }

    EventType event_type() const { return netidx_subscriber_update_event_type(ptr_); }

    Value value_clone() const {
        auto* p = netidx_subscriber_update_value_clone(ptr_);
        if (!p) throw Error("update has no value (Unsubscribed)");
        return Value(p);
    }

    ~SubscriberUpdate() { if (ptr_) netidx_subscriber_update_destroy(ptr_); }

    SubscriberUpdate(const SubscriberUpdate&) = delete;
    SubscriberUpdate& operator=(const SubscriberUpdate&) = delete;
    SubscriberUpdate(SubscriberUpdate&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    SubscriberUpdate& operator=(SubscriberUpdate&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_subscriber_update_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit SubscriberUpdate(NetidxSubscriberUpdate* p) : ptr_(p) {}
    friend class UpdateReceiver;
};

// --- UpdateBatch ---

class UpdateBatch {
    NetidxUpdateBatch* ptr_;
public:
    void commit(const Runtime& rt, int64_t timeout_ms = -1) {
        netidx_update_batch_commit(rt.raw(), ptr_, timeout_ms);
        ptr_ = nullptr; // consumed
    }

    ~UpdateBatch() { if (ptr_) netidx_update_batch_destroy(ptr_); }

    UpdateBatch(const UpdateBatch&) = delete;
    UpdateBatch& operator=(const UpdateBatch&) = delete;
    UpdateBatch(UpdateBatch&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    UpdateBatch& operator=(UpdateBatch&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_update_batch_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    NetidxUpdateBatch* raw() { return ptr_; }

private:
    explicit UpdateBatch(NetidxUpdateBatch* p) : ptr_(p) {}
    friend class Publisher;
};

// --- PublisherVal ---

class PublisherVal {
    NetidxPublisherVal* ptr_;
public:
    void update(UpdateBatch& batch, Value value) {
        netidx_publisher_val_update(ptr_, batch.raw(), value.release());
    }

    void update_changed(UpdateBatch& batch, Value value) {
        netidx_publisher_val_update_changed(ptr_, batch.raw(), value.release());
    }

    ~PublisherVal() { if (ptr_) netidx_publisher_val_destroy(ptr_); }

    PublisherVal(const PublisherVal&) = delete;
    PublisherVal& operator=(const PublisherVal&) = delete;
    PublisherVal(PublisherVal&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    PublisherVal& operator=(PublisherVal&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_publisher_val_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxPublisherVal* raw() const { return ptr_; }

private:
    explicit PublisherVal(NetidxPublisherVal* p) : ptr_(p) {}
    friend class Publisher;
};

// --- WriteRequest ---

class WriteRequest {
    NetidxWriteRequest* ptr_;
public:
    std::string_view path() const {
        const char* data; size_t len;
        netidx_write_request_path(ptr_, &data, &len);
        return {data, len};
    }

    Value value() const {
        return Value(netidx_write_request_value(ptr_));
    }

    ~WriteRequest() { if (ptr_) netidx_write_request_destroy(ptr_); }

    WriteRequest(const WriteRequest&) = delete;
    WriteRequest& operator=(const WriteRequest&) = delete;
    WriteRequest(WriteRequest&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    WriteRequest& operator=(WriteRequest&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_write_request_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit WriteRequest(NetidxWriteRequest* p) : ptr_(p) {}
    friend class WriteReceiver;
};

// --- WriteReceiver ---

class WriteReceiver {
    NetidxWriteReceiver* ptr_;
public:
    std::vector<WriteRequest> try_recv() {
        size_t len;
        auto** arr = netidx_write_receiver_try_recv(ptr_, &len);
        std::vector<WriteRequest> result;
        if (arr) {
            result.reserve(len);
            for (size_t i = 0; i < len; ++i)
                result.emplace_back(WriteRequest(arr[i]));
            netidx_write_request_array_free(arr, len);
        }
        return result;
    }

    std::vector<WriteRequest> recv(const Runtime& rt, int64_t timeout_ms = -1) {
        size_t len;
        auto** arr = netidx_write_receiver_recv(rt.raw(), ptr_, timeout_ms, &len);
        std::vector<WriteRequest> result;
        if (arr) {
            result.reserve(len);
            for (size_t i = 0; i < len; ++i)
                result.emplace_back(WriteRequest(arr[i]));
            netidx_write_request_array_free(arr, len);
        }
        return result;
    }

    ~WriteReceiver() { if (ptr_) netidx_write_receiver_destroy(ptr_); }

    WriteReceiver(const WriteReceiver&) = delete;
    WriteReceiver& operator=(const WriteReceiver&) = delete;
    WriteReceiver(WriteReceiver&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    WriteReceiver& operator=(WriteReceiver&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_write_receiver_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit WriteReceiver(NetidxWriteReceiver* p) : ptr_(p) {}
    friend class Publisher;
};

// --- Publisher ---

class Publisher {
    NetidxPublisher* ptr_;
public:
    PublisherVal publish(Path path, Value init) {
        NetidxError err = netidx_error_init();
        auto* val = netidx_publisher_publish(ptr_, path.release(), init.release(), &err);
        detail::check_error(err);
        return PublisherVal(val);
    }

    struct PublishWithWritesResult {
        PublisherVal val;
        WriteReceiver rx;
    };

    PublishWithWritesResult publish_with_writes(Path path, Value init, size_t channel_buffer = 0) {
        NetidxError err = netidx_error_init();
        NetidxWriteReceiver* rx_ptr = nullptr;
        auto* val = netidx_publisher_publish_with_writes(
            ptr_, path.release(), init.release(), channel_buffer, &rx_ptr, &err);
        detail::check_error(err);
        return {PublisherVal(val), WriteReceiver(rx_ptr)};
    }

    WriteReceiver writes(const PublisherVal& val, size_t channel_buffer = 0) {
        return WriteReceiver(netidx_publisher_writes(ptr_, val.raw(), channel_buffer));
    }

    UpdateBatch start_batch() {
        return UpdateBatch(netidx_publisher_start_batch(ptr_));
    }

    void flushed(const Runtime& rt) {
        netidx_publisher_flushed(rt.raw(), ptr_);
    }

    ~Publisher() { if (ptr_) netidx_publisher_destroy(ptr_); }

    Publisher(const Publisher& o) : ptr_(netidx_publisher_clone(o.ptr_)) {}
    Publisher& operator=(const Publisher& o) {
        if (this != &o) { if (ptr_) netidx_publisher_destroy(ptr_); ptr_ = netidx_publisher_clone(o.ptr_); }
        return *this;
    }
    Publisher(Publisher&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Publisher& operator=(Publisher&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_publisher_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxPublisher* raw() const { return ptr_; }

private:
    explicit Publisher(NetidxPublisher* p) : ptr_(p) {}
    friend class PublisherBuilder;
};

// --- PublisherBuilder ---

class PublisherBuilder {
    NetidxPublisherBuilder* ptr_;
public:
    explicit PublisherBuilder(const Config& cfg)
        : ptr_(netidx_publisher_builder_new(cfg.raw())) {}

    PublisherBuilder& bind_cfg(std::string_view bind) {
        NetidxError err = netidx_error_init();
        netidx_publisher_builder_bind_cfg(ptr_, bind.data(), bind.size(), &err);
        detail::check_error(err);
        return *this;
    }

    PublisherBuilder& max_clients(size_t n) {
        netidx_publisher_builder_max_clients(ptr_, n);
        return *this;
    }

    PublisherBuilder& slack(size_t n) {
        netidx_publisher_builder_slack(ptr_, n);
        return *this;
    }

    Publisher build(const Runtime& rt) {
        NetidxError err = netidx_error_init();
        auto* p = netidx_publisher_builder_build(rt.raw(), ptr_, &err);
        ptr_ = nullptr; // consumed
        detail::check_error(err);
        return Publisher(p);
    }

    ~PublisherBuilder() { if (ptr_) netidx_publisher_builder_destroy(ptr_); }

    PublisherBuilder(const PublisherBuilder&) = delete;
    PublisherBuilder& operator=(const PublisherBuilder&) = delete;
    PublisherBuilder(PublisherBuilder&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    PublisherBuilder& operator=(PublisherBuilder&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_publisher_builder_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }
};

// --- UpdateReceiver ---

class UpdateReceiver {
    NetidxUpdateReceiver* ptr_;
public:
    std::vector<SubscriberUpdate> try_recv() {
        size_t len;
        auto** arr = netidx_update_receiver_try_recv(ptr_, &len);
        std::vector<SubscriberUpdate> result;
        if (arr) {
            result.reserve(len);
            for (size_t i = 0; i < len; ++i)
                result.emplace_back(SubscriberUpdate(arr[i]));
            netidx_subscriber_update_array_free(arr, len);
        }
        return result;
    }

    std::vector<SubscriberUpdate> recv(const Runtime& rt, int64_t timeout_ms = -1) {
        size_t len;
        auto** arr = netidx_update_receiver_recv(rt.raw(), ptr_, timeout_ms, &len);
        std::vector<SubscriberUpdate> result;
        if (arr) {
            result.reserve(len);
            for (size_t i = 0; i < len; ++i)
                result.emplace_back(SubscriberUpdate(arr[i]));
            netidx_subscriber_update_array_free(arr, len);
        }
        return result;
    }

    ~UpdateReceiver() { if (ptr_) netidx_update_receiver_destroy(ptr_); }

    UpdateReceiver(const UpdateReceiver&) = delete;
    UpdateReceiver& operator=(const UpdateReceiver&) = delete;
    UpdateReceiver(UpdateReceiver&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    UpdateReceiver& operator=(UpdateReceiver&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_update_receiver_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit UpdateReceiver(NetidxUpdateReceiver* p) : ptr_(p) {}
    friend class Subscriber;
};

// --- Dval ---

class Dval {
    NetidxDval* ptr_;
public:
    void wait_subscribed(const Runtime& rt, int64_t timeout_ms = -1) {
        NetidxError err = netidx_error_init();
        netidx_dval_wait_subscribed(rt.raw(), ptr_, timeout_ms, &err);
        detail::check_error(err);
    }

    Event last() const {
        return Event(netidx_dval_last(ptr_));
    }

    uint64_t id() const {
        return netidx_dval_id(ptr_);
    }

    bool write(Value value) {
        return netidx_dval_write(ptr_, value.release());
    }

    ~Dval() { if (ptr_) netidx_dval_destroy(ptr_); }

    Dval(const Dval& o) : ptr_(netidx_dval_clone(o.ptr_)) {}
    Dval& operator=(const Dval& o) {
        if (this != &o) { if (ptr_) netidx_dval_destroy(ptr_); ptr_ = netidx_dval_clone(o.ptr_); }
        return *this;
    }
    Dval(Dval&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Dval& operator=(Dval&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_dval_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxDval* raw() const { return ptr_; }

private:
    explicit Dval(NetidxDval* p) : ptr_(p) {}
    friend class Subscriber;
};

// --- Subscriber ---

class Subscriber {
    NetidxSubscriber* ptr_;
public:
    Dval subscribe(const Path& path) {
        return Dval(netidx_subscriber_subscribe(ptr_, path.raw()));
    }

    struct SubscribeUpdatesResult {
        Dval dval;
        UpdateReceiver rx;
    };

    SubscribeUpdatesResult subscribe_updates(const Path& path, size_t channel_buffer = 0) {
        NetidxUpdateReceiver* rx_ptr = nullptr;
        auto* dval = netidx_subscriber_subscribe_updates(ptr_, path.raw(), channel_buffer, &rx_ptr);
        return {Dval(dval), UpdateReceiver(rx_ptr)};
    }

    ~Subscriber() { if (ptr_) netidx_subscriber_destroy(ptr_); }

    Subscriber(const Subscriber& o) : ptr_(netidx_subscriber_clone(o.ptr_)) {}
    Subscriber& operator=(const Subscriber& o) {
        if (this != &o) { if (ptr_) netidx_subscriber_destroy(ptr_); ptr_ = netidx_subscriber_clone(o.ptr_); }
        return *this;
    }
    Subscriber(Subscriber&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Subscriber& operator=(Subscriber&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_subscriber_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

    const NetidxSubscriber* raw() const { return ptr_; }

private:
    explicit Subscriber(NetidxSubscriber* p) : ptr_(p) {}
    friend class SubscriberBuilder;
};

// --- SubscriberBuilder ---

class SubscriberBuilder {
    NetidxSubscriberBuilder* ptr_;
public:
    explicit SubscriberBuilder(const Config& cfg)
        : ptr_(netidx_subscriber_builder_new(cfg.raw())) {}

    Subscriber build() {
        NetidxError err = netidx_error_init();
        auto* p = netidx_subscriber_builder_build(ptr_, &err);
        ptr_ = nullptr; // consumed
        detail::check_error(err);
        return Subscriber(p);
    }

    ~SubscriberBuilder() { if (ptr_) netidx_subscriber_builder_destroy(ptr_); }

    SubscriberBuilder(const SubscriberBuilder&) = delete;
    SubscriberBuilder& operator=(const SubscriberBuilder&) = delete;
    SubscriberBuilder(SubscriberBuilder&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    SubscriberBuilder& operator=(SubscriberBuilder&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_subscriber_builder_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }
};

// --- RPC ---

namespace rpc {

// --- RPC Call (server side) ---

class Call {
    NetidxRpcCall* ptr_;
public:
    Value take_arg(std::string_view name) {
        auto* v = netidx_rpc_call_take_arg(ptr_, name.data(), name.size());
        if (!v) throw Error("rpc argument not found: " + std::string(name));
        return Value(v);
    }

    void reply(Value value) {
        netidx_rpc_call_reply(ptr_, value.release());
        ptr_ = nullptr; // consumed
    }

    ~Call() { if (ptr_) netidx_rpc_call_destroy(ptr_); }

    Call(const Call&) = delete;
    Call& operator=(const Call&) = delete;
    Call(Call&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Call& operator=(Call&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_rpc_call_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit Call(NetidxRpcCall* p) : ptr_(p) {}
    friend class Proc;
    friend extern "C" void rpc_proc_c_handler(void*, NetidxRpcCall*);
};

// Type-erased C callback context for RPC proc handlers
struct ProcCallbackCtx {
    std::function<void(Call)> fn;
};

extern "C" inline void rpc_proc_c_handler(void* userdata, NetidxRpcCall* raw_call) {
    auto* ctx = static_cast<ProcCallbackCtx*>(userdata);
    ctx->fn(Call(raw_call));
}

// --- RPC Server Proc ---

class Proc {
    NetidxRpcProc* ptr_;
    ProcCallbackCtx* ctx_ = nullptr;

public:
    struct ArgSpec {
        std::string_view name;
        Value doc;
        Value default_value;
    };

    static Proc create(
        const Publisher& pub_,
        Path path,
        Value doc,
        std::span<ArgSpec> args,
        std::function<void(Call)> handler
    ) {
        auto* ctx = new ProcCallbackCtx{std::move(handler)};
        std::vector<NetidxArgSpec> c_args;
        c_args.reserve(args.size());
        for (auto& a : args) {
            c_args.push_back({
                a.name.data(),
                a.name.size(),
                netidx_value_clone(a.doc.raw()),
                netidx_value_clone(a.default_value.raw()),
            });
        }
        NetidxError err = netidx_error_init();
        auto* proc = netidx_rpc_proc_new(
            pub_.raw(), path.release(), netidx_value_clone(doc.raw()),
            c_args.data(), c_args.size(),
            rpc_proc_c_handler, ctx, &err);
        if (netidx_error_message(&err)) {
            delete ctx;
            detail::check_error(err);
        }
        Proc p(proc);
        p.ctx_ = ctx;
        return p;
    }

    ~Proc() {
        if (ptr_) netidx_rpc_proc_destroy(ptr_);
        delete ctx_;
    }

    Proc(const Proc&) = delete;
    Proc& operator=(const Proc&) = delete;
    Proc(Proc&& o) noexcept : ptr_(o.ptr_), ctx_(o.ctx_) { o.ptr_ = nullptr; o.ctx_ = nullptr; }
    Proc& operator=(Proc&& o) noexcept {
        if (this != &o) {
            if (ptr_) netidx_rpc_proc_destroy(ptr_);
            delete ctx_;
            ptr_ = o.ptr_; ctx_ = o.ctx_;
            o.ptr_ = nullptr; o.ctx_ = nullptr;
        }
        return *this;
    }

private:
    explicit Proc(NetidxRpcProc* p) : ptr_(p) {}
};

// --- RPC Client ---

class Client {
    NetidxRpcClient* ptr_;
public:
    Client(const Subscriber& sub, const Path& path) {
        NetidxError err = netidx_error_init();
        ptr_ = netidx_rpc_client_new(sub.raw(), path.raw(), &err);
        detail::check_error(err);
    }

    struct Arg {
        std::string_view name;
        Value value;
    };

    Value call(const Runtime& rt, std::span<Arg> args, int64_t timeout_ms = -1) {
        std::vector<NetidxRpcArg> c_args;
        c_args.reserve(args.size());
        for (auto& a : args) {
            c_args.push_back({
                a.name.data(),
                a.name.size(),
                netidx_value_clone(a.value.raw()),
            });
        }
        NetidxError err = netidx_error_init();
        auto* v = netidx_rpc_client_call(rt.raw(), ptr_, c_args.data(), c_args.size(), timeout_ms, &err);
        detail::check_error(err);
        return Value(v);
    }

    ~Client() { if (ptr_) netidx_rpc_client_destroy(ptr_); }

    Client(const Client& o) : ptr_(netidx_rpc_client_clone(o.ptr_)) {}
    Client& operator=(const Client& o) {
        if (this != &o) { if (ptr_) netidx_rpc_client_destroy(ptr_); ptr_ = netidx_rpc_client_clone(o.ptr_); }
        return *this;
    }
    Client(Client&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Client& operator=(Client&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_rpc_client_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }
};

} // namespace rpc

// --- Channel ---

namespace channel {

// --- Server Connection ---

class ServerConnection {
    NetidxChannelServerConn* ptr_;
public:
    void send_one(const Runtime& rt, Value value) {
        NetidxError err = netidx_error_init();
        netidx_channel_server_conn_send_one(rt.raw(), ptr_, value.release(), &err);
        detail::check_error(err);
    }

    Value recv_one(const Runtime& rt, int64_t timeout_ms = -1) {
        NetidxError err = netidx_error_init();
        auto* v = netidx_channel_server_conn_recv_one(rt.raw(), ptr_, timeout_ms, &err);
        detail::check_error(err);
        return Value(v);
    }

    bool is_dead() const { return netidx_channel_server_conn_is_dead(ptr_); }

    ~ServerConnection() { if (ptr_) netidx_channel_server_conn_destroy(ptr_); }

    ServerConnection(const ServerConnection&) = delete;
    ServerConnection& operator=(const ServerConnection&) = delete;
    ServerConnection(ServerConnection&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    ServerConnection& operator=(ServerConnection&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_channel_server_conn_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit ServerConnection(NetidxChannelServerConn* p) : ptr_(p) {}
    friend class Listener;
};

// --- Listener ---

class Listener {
    NetidxChannelListener* ptr_;
public:
    Listener(const Runtime& rt, const Publisher& pub_, const Path& path,
             int64_t timeout_ms = -1) {
        NetidxError err = netidx_error_init();
        ptr_ = netidx_channel_listener_new(rt.raw(), pub_.raw(), path.raw(), timeout_ms, &err);
        detail::check_error(err);
    }

    ServerConnection accept(const Runtime& rt, int64_t timeout_ms = -1) {
        NetidxError err = netidx_error_init();
        auto* conn = netidx_channel_listener_accept(rt.raw(), ptr_, timeout_ms, &err);
        detail::check_error(err);
        return ServerConnection(conn);
    }

    ~Listener() { if (ptr_) netidx_channel_listener_destroy(ptr_); }

    Listener(const Listener&) = delete;
    Listener& operator=(const Listener&) = delete;
    Listener(Listener&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    Listener& operator=(Listener&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_channel_listener_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }
};

// --- Client Connection ---

class ClientConnection {
    NetidxChannelClientConn* ptr_;
public:
    static ClientConnection connect(const Runtime& rt, const Subscriber& sub,
                                    const Path& path) {
        NetidxError err = netidx_error_init();
        auto* conn = netidx_channel_client_connect(rt.raw(), sub.raw(), path.raw(), &err);
        detail::check_error(err);
        return ClientConnection(conn);
    }

    void send(Value value) {
        NetidxError err = netidx_error_init();
        netidx_channel_client_conn_send(ptr_, value.release(), &err);
        detail::check_error(err);
    }

    void flush(const Runtime& rt) {
        NetidxError err = netidx_error_init();
        netidx_channel_client_conn_flush(rt.raw(), ptr_, &err);
        detail::check_error(err);
    }

    Value recv_one(const Runtime& rt, int64_t timeout_ms = -1) {
        NetidxError err = netidx_error_init();
        auto* v = netidx_channel_client_conn_recv_one(rt.raw(), ptr_, timeout_ms, &err);
        detail::check_error(err);
        return Value(v);
    }

    bool is_dead() const { return netidx_channel_client_conn_is_dead(ptr_); }

    ~ClientConnection() { if (ptr_) netidx_channel_client_conn_destroy(ptr_); }

    ClientConnection(const ClientConnection&) = delete;
    ClientConnection& operator=(const ClientConnection&) = delete;
    ClientConnection(ClientConnection&& o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
    ClientConnection& operator=(ClientConnection&& o) noexcept {
        if (this != &o) { if (ptr_) netidx_channel_client_conn_destroy(ptr_); ptr_ = o.ptr_; o.ptr_ = nullptr; }
        return *this;
    }

private:
    explicit ClientConnection(NetidxChannelClientConn* p) : ptr_(p) {}
};

} // namespace channel

} // namespace netidx
