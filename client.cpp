#include <condition_variable>
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
    bool done_ = false;
};

class StreamReactor final : public ClientBidiReactor<Request, Response> {
public:
    StreamReactor(BidirClient* owner, std::unique_ptr<ClientContext> context)
        : owner_(owner), context_(std::move(context)) {
        Request r;
        r.set_opcode(Opcode::PING);
        r.set_id(1);
        requests_.push_back(r);

        Request d;
        d.set_opcode(Opcode::DATA);
        d.set_id(2);
        d.set_payload("Hello Server");
        requests_.push_back(d);
    }

    void KickOff(BidirService::Stub* stub) {
        // Start the RPC with this reactor bound to it.
        stub->experimental_async()->Stream(context_.get(), this);
        // Post the initial read so responses are awaited as soon as the call starts.
        StartRead(&response_);
        WriteNext();
        StartCall();
    }

    void WriteNext() {
        if (write_idx_ < requests_.size()) {
            request_ = requests_[write_idx_++];
            StartWrite(&request_);
        } else {
            StartWritesDone();
        }
    }

    void OnWriteDone(bool ok) override {
        if (ok) {
            WriteNext();
        }
    }

    void OnWritesDoneDone(bool ok) override {
        // All requests sent; the server decides when to close the stream.
        (void)ok;
    }

    void OnReadDone(bool ok) override {
        if (!ok) return;  // No more responses; await OnDone.

        // Placeholder: Extract Opcode and dispatch response
        Opcode op = response_.opcode();
        std::cout << "[" << owner_->address() << "] Received response with opcode: " << op
                  << ", message: " << response_.message() << std::endl;
        StartRead(&response_);
    }

    void OnDone(const Status& s) override {
        std::cout << "[" << owner_->address() << "] Stream finished: "
                  << (s.ok() ? "OK" : s.error_message()) << std::endl;
        owner_->SignalDone();
        delete this;
    }

private:
    BidirClient* owner_;
    std::unique_ptr<ClientContext> context_;
    std::vector<Request> requests_;
    std::size_t write_idx_ = 0;
    Request request_;    // Buffer for the write in flight.
    Response response_;  // Buffer for the read in flight.
};

BidirClient::BidirClient(const std::string& address)
    : address_(address),
      stub_(BidirService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

void BidirClient::Stream() {
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = false;
    }
    std::unique_ptr<ClientContext> context = std::make_unique<ClientContext>();
    StreamReactor* reactor = new StreamReactor(this, std::move(context));
    reactor->KickOff(stub_.get());

    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return done_; });
}

void BidirClient::SignalDone() {
    std::lock_guard<std::mutex> lk(mu_);
    done_ = true;
    cv_.notify_one();
}

int main(int argc, char** argv) {
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
        std::cout << "Connecting to server at " << address << std::endl;
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