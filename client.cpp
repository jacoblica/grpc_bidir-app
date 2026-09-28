#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
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

std::atomic<bool> g_stop{false};

void HandleSignal(int) { g_stop.store(true); }

}  // namespace

class BidirClient;
class StreamReactor;

class BidirClient {
public:
    explicit BidirClient(const std::string& address);
    void Stream();
    const std::string& address() const { return address_; }
    void SignalDone();

private:
    std::string address_;
    std::unique_ptr<BidirService::Stub> stub_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::shared_ptr<ClientContext> context_;
    bool done_ = false;
};

class StreamReactor final : public ClientBidiReactor<Request, Response> {
public:
    StreamReactor(BidirClient* owner, std::shared_ptr<ClientContext> context)
        : owner_(owner), context_(std::move(context)) {}

    void KickOff(BidirService::Stub* stub) {
        // Start the RPC with this reactor bound to it.
        stub->experimental_async()->Stream(context_.get(), this);
        // Post the initial read so responses are awaited as soon as the call starts.
        StartRead(&response_);
        // The client is read-only on this stream: it never starts a write and
        // never half-closes, so the server keeps pushing until the call ends.
        StartCall();
    }

    void OnReadDone(bool ok) override {
        if (!ok) return;  // No more responses; await OnDone.

        // The server pushes unsolicited STATUS updates carrying a date/time string.
        std::cout << "[" << owner_->address() << "] #" << response_.id() << " "
                  << FormatMessage(response_) << std::endl;
        StartRead(&response_);
    }

    void OnDone(const Status& s) override {
        std::cout << "[" << owner_->address() << "] Stream finished: "
                  << (s.ok() ? "OK" : s.error_message()) << std::endl;
        owner_->SignalDone();
        delete this;
    }

private:
    static std::string FormatMessage(const Response& response) {
        return response.message().empty() ? std::string("<empty>") : response.message();
    }

    BidirClient* owner_;
    std::shared_ptr<ClientContext> context_;
    Response response_;  // Buffer for the read in flight.
};

BidirClient::BidirClient(const std::string& address)
    : address_(address),
      stub_(BidirService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

void BidirClient::Stream() {
    std::unique_lock<std::mutex> lk(mu_);
    done_ = false;
    context_ = std::make_shared<ClientContext>();

    StreamReactor* reactor = new StreamReactor(this, context_);
    reactor->KickOff(stub_.get());

    // Idle until the server closes the stream, or until the user interrupts.
    while (!done_ && !g_stop.load()) {
        cv_.wait_for(lk, std::chrono::milliseconds(100));
    }
    if (!done_) {
        // Interrupted: cancel the call and keep waiting for OnDone, since the
        // reactor still holds a pointer to this object.
        context_->TryCancel();
        cv_.wait(lk, [this] { return done_; });
    }
    context_.reset();
}

void BidirClient::SignalDone() {
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

    std::cout << "Connecting to server at " << addresses.front()
              << "; press Ctrl-C to stop." << std::endl;

    std::vector<std::thread> threads;
    for (const auto& address : addresses) {
        threads.emplace_back([address]() {
            BidirClient client(address);
            client.Stream();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}
