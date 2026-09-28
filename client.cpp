#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::CallbackServerContext;
using grpc::Server;
using grpc::ServerBidiReactor;
using grpc::ServerBuilder;
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

// The client now listens; the server dials in, so identify each accepted stream.
namespace {
std::atomic<int> g_next_conn_id{0};
}  // namespace

// Reactor type order is <inbound, outbound>: the listening side reads Response
// messages and writes Request messages.
class StreamReactor final : public ServerBidiReactor<Response, Request> {
public:
    StreamReactor() : conn_id_(g_next_conn_id++) {
        Request ping;
        ping.set_opcode(Opcode::PING);
        ping.set_id(1);
        requests_.push_back(ping);

        Request data;
        data.set_opcode(Opcode::DATA);
        data.set_id(2);
        data.set_payload("Hello Server");
        requests_.push_back(data);

        // Await responses and send our first request.
        StartRead(&response_);
        SendNext();
    }

    void OnReadDone(bool ok) override {
        if (finished_) return;
        if (!ok) {
            // The server closed the stream.
            Finish(Status::OK);
            finished_ = true;
            return;
        }

        // Placeholder: Extract Opcode and dispatch response
        Opcode op = response_.opcode();
        std::cout << "[conn " << conn_id_ << "] Received response with opcode: " << op
                  << ", message: " << response_.message() << std::endl;

        ++responses_received_;
        if (responses_received_ < requests_.size()) {
            StartRead(&response_);
            return;
        }
        // Every request has been answered; close the call like the blocking
        // version did once its reader thread finished.
        MaybeFinish();
    }

    void OnWriteDone(bool ok) override {
        if (finished_) return;
        if (!ok) {
            Finish(Status(grpc::StatusCode::INTERNAL, "Write failed"));
            finished_ = true;
            return;
        }
        write_in_flight_ = false;
        if (write_idx_ < requests_.size()) {
            SendNext();
            return;
        }
        MaybeFinish();
    }

    void OnCancel() override {
        if (!finished_) {
            Finish(Status(grpc::StatusCode::CANCELLED, "Call cancelled"));
            finished_ = true;
        }
    }

    void OnDone() override {
        // RPC is fully complete; safe to reclaim the reactor.
        delete this;
    }

private:
    void SendNext() {
        // StartWriteLast is deliberately not used: it defers the message so it can
        // be coalesced with the trailing metadata of Finish, which would stall the
        // peer waiting for a request that never leaves this process.
        request_ = requests_[write_idx_++];
        write_in_flight_ = true;
        StartWrite(&request_);
    }

    // The server role has no WritesDone, so the call is closed here once every
    // request has been sent and every response has come back, and no operation
    // is still in flight.
    void MaybeFinish() {
        if (finished_ || write_in_flight_) return;
        if (write_idx_ < requests_.size() || responses_received_ < requests_.size()) return;
        Finish(Status::OK);
        finished_ = true;
    }

    int conn_id_;
    std::vector<Request> requests_;
    std::size_t write_idx_ = 0;
    std::size_t responses_received_ = 0;
    Request request_;    // Buffer for the write in flight.
    Response response_;  // Buffer for the read in flight.
    bool write_in_flight_ = false;
    bool finished_ = false;
};

class BidirServiceImpl final : public BidirService::CallbackService {
    ServerBidiReactor<Response, Request>* Stream(CallbackServerContext* context) override {
        std::cout << "Server connected, starting stream" << std::endl;
        return new StreamReactor();
    }
};

void RunListener(const std::string& listen_address) {
    BidirServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server) {
        std::cout << "Failed to listen on " << listen_address << std::endl;
        return;
    }
    std::cout << "Client listening on " << listen_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::vector<std::string> addresses;
    if (argc > 1) {
        for (int i = 1; i < argc; ++i) {
            addresses.push_back(argv[i]);
        }
    } else {
        addresses.push_back("0.0.0.0:50051");
    }

    std::vector<std::thread> threads;
    for (const auto& address : addresses) {
        threads.emplace_back([address]() { RunListener(address); });
    }

    for (auto& t : threads) {
        t.join();
    }
    return 0;
}