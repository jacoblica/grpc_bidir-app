#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::CallbackServerContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerBidiReactor;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

class BidirServiceImpl;

// This process is the *listener*: it hosts the gRPC server and only ever
// receives. The pushing peer connects to it and writes date/time messages.
class StreamReactor final : public ServerBidiReactor<Request, Response> {
public:
    StreamReactor() {
        // Nothing is ever written back; arm a read to receive the pushed messages.
        StartRead(&request_);
    }

    void OnReadDone(bool ok) override {
        if (finished_) return;
        if (!ok) {
            // Push source closed its write side or went away.
            std::cout << "Push source disconnected" << std::endl;
            finished_ = true;
            Finish(Status::OK);
            return;
        }

        // The peer pushes unsolicited STATUS updates carrying a date/time string.
        std::cout << "#" << request_.id() << " " << request_.payload() << std::endl;
        StartRead(&request_);
    }

    void OnCancel() override {
        if (!finished_) {
            finished_ = true;
            Finish(Status(grpc::StatusCode::CANCELLED, "Call cancelled"));
        }
    }

    void OnDone() override {
        // RPC is fully complete; safe to reclaim the reactor.
        delete this;
    }

private:
    Request request_;  // Buffer for the read in flight.
    bool finished_ = false;
};

class BidirServiceImpl final : public BidirService::CallbackService {
    ServerBidiReactor<Request, Response>* Stream(CallbackServerContext* context) override {
        std::cout << "Push source connected" << std::endl;
        return new StreamReactor();
    }
};

void RunListener(const std::string& listen_address) {
    // Block the signals here so the dedicated thread below can wait for them
    // safely; Shutdown() must not be called from a signal handler.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    BidirServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (server == nullptr) {
        std::cerr << "Failed to listen on " << listen_address << std::endl;
        return;
    }

    std::thread signal_thread([&server, &signals]() {
        int signal_number = 0;
        if (sigwait(&signals, &signal_number) == 0) {
            std::cout << "Received signal " << signal_number << ", shutting down" << std::endl;
            server->Shutdown();
        }
    });

    std::cout << "Listening on " << listen_address << "; waiting for push sources. Ctrl-C to stop."
              << std::endl;
    server->Wait();
    signal_thread.join();
    std::cout << "Listener stopped" << std::endl;
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunListener(address);
    return 0;
}
