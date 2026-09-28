#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::ClientBidiReactor;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

namespace {

constexpr auto kPushInterval = std::chrono::seconds(3);

std::atomic<bool> g_stop{false};
std::mutex g_log_mu;

void HandleSignal(int) { g_stop.store(true); }

// Each address runs on its own thread; keep the shared stdout from interleaving.
void Log(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_log_mu);
    std::cout << line << std::endl;
}

std::string FormatTimestamp() {
    const std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm_buf{};
    localtime_r(&now, &tm_buf);
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

}  // namespace

class StreamReactor;

// One pushing connection per target address.
class PushClient {
public:
    explicit PushClient(const std::string& address);
    void Run();
    const std::string& address() const { return address_; }
    void SignalDone();

private:
    std::string address_;
    std::unique_ptr<BidirService::Stub> stub_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool done_ = false;
};

// This process is the *pusher*: it dials out to the listener and is the only
// writer on the stream, sending a date/time message every 3 seconds.
class StreamReactor final : public ClientBidiReactor<Request, Response> {
public:
    StreamReactor(PushClient* owner, std::shared_ptr<ClientContext> context)
        : owner_(owner), context_(std::move(context)) {}

    void KickOff(BidirService::Stub* stub) {
        // Start the RPC with this reactor bound to it.
        stub->experimental_async()->Stream(context_.get(), this);
        // The listener never writes; this read only detects the stream ending.
        StartRead(&response_);
        StartCall();
        // Per-connection ticker that pushes the clock on a fixed cadence.
        ticker_ = std::thread([this] { TickerLoop(); });
    }

    void OnReadDone(bool ok) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        if (ok) {
            // Nothing to dispatch: this side is the only writer.
            StartRead(&response_);
            return;
        }
        // Listener closed the stream or disconnected: stop pushing and cancel
        // the call so OnDone runs and the owner can return.
        stopped_ = true;
        cv_.notify_all();
        context_->TryCancel();
    }

    void OnWriteDone(bool ok) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        write_in_progress_ = false;
        if (!ok) {
            stopped_ = true;
            cv_.notify_all();
            context_->TryCancel();
        }
    }

    void OnDone(const Status& s) override {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stopped_ = true;
        }
        cv_.notify_all();
        if (ticker_.joinable()) ticker_.join();
        Log("[" + owner_->address() + "] Stream finished: " +
            (s.ok() ? "OK" : s.error_message()));
        owner_->SignalDone();
        delete this;
    }

private:
    void TickerLoop() {
        std::unique_lock<std::mutex> lk(mu_);
        while (!cv_.wait_for(lk, kPushInterval, [this] { return stopped_ || g_stop.load(); })) {
            if (write_in_progress_) {
                // request_ is owned by the in-flight write; try again next tick.
                continue;
            }
            ++pushed_;
            request_.set_opcode(Opcode::STATUS);
            request_.set_id(pushed_);
            request_.set_payload(FormatTimestamp());
            request_.set_active(true);
            request_.set_value(0.0F);
            write_in_progress_ = true;
            StartWrite(&request_);
        }
    }

    PushClient* owner_;
    std::shared_ptr<ClientContext> context_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread ticker_;
    bool stopped_ = false;
    bool write_in_progress_ = false;
    Request request_;   // Buffer for the write in flight.
    Response response_; // Buffer for the read in flight.
    int32_t pushed_ = 0;
};

PushClient::PushClient(const std::string& address)
    : address_(address),
      stub_(BidirService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

void PushClient::Run() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = false;
    }
    std::shared_ptr<ClientContext> context = std::make_shared<ClientContext>();

    StreamReactor* reactor = new StreamReactor(this, context);
    reactor->KickOff(stub_.get());
    Log("[" + address_ + "] Connected, pushing date/time every " + std::to_string(kPushInterval.count()) +
        "s; press Ctrl-C to stop.");

    // Idle until the stream ends, or until the user interrupts.
    std::unique_lock<std::mutex> lk(mu_);
    while (!done_ && !g_stop.load()) {
        cv_.wait_for(lk, std::chrono::milliseconds(100));
    }
    if (!done_) {
        // Interrupted: cancel the call and keep waiting for OnDone, since the
        // reactor still holds a pointer to this object.
        context->TryCancel();
        cv_.wait(lk, [this] { return done_; });
    }
}

void PushClient::SignalDone() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
    }
    cv_.notify_all();
}

int main(int argc, char** argv) {
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::vector<std::string> addresses;
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) {
            addresses.push_back(argv[i]);
        }
    } else {
        addresses.push_back("localhost:50051");
    }

    std::vector<std::thread> threads;
    for (const auto& address : addresses) {
        threads.emplace_back([address]() {
            PushClient client(address);
            client.Run();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}
