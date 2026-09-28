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
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

class BidirClient;

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

// The server now dials out to the listening client, so a reactor per call.
// Reactor type order is <outbound, inbound>: the dialing side writes Response
// messages and reads Request messages.
class StreamReactor final : public ClientBidiReactor<Response, Request> {
public:
    StreamReactor(BidirClient* owner, std::unique_ptr<ClientContext> context)
        : owner_(owner), context_(std::move(context)) {}

    void KickOff(BidirService::Stub* stub) {
        // Start the RPC with this reactor bound to it.
        stub->experimental_async()->Stream(context_.get(), this);
        // Await the client's requests.
        StartRead(&request_);
        StartCall();
    }

    void OnReadDone(bool ok) override {
        if (!ok) {
            // The client half-closed; nothing more to respond to.
            return;
        }

        // Placeholder: Extract Opcode and dispatch
        Opcode op = request_.opcode();
        std::cout << "Received request with opcode: " << op << std::endl;

        response_.set_opcode(op);
        response_.set_id(request_.id());

        switch (op) {
            case bidir::Opcode::PING:
                // TODO: Handle PING
                response_.set_message("PONG");
                break;
            case bidir::Opcode::DATA:
                // TODO: Handle DATA
                response_.set_message("DATA RECEIVED");
                break;
            default:
                response_.set_message("UNKNOWN OPCODE");
                break;
        }

        StartWrite(&response_);
    }

    void OnWriteDone(bool ok) override {
        if (!ok) return;
        StartRead(&request_);
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
    Request request_;   // Buffer for the read in flight.
    Response response_; // Buffer for the write in flight.
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
        std::cout << "Connecting to client at " << address << std::endl;
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