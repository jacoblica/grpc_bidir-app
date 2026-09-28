#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

namespace {

std::mutex g_log_mu;

// Each stream is handled on its own gRPC sync thread; keep stdout readable.
void Log(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_log_mu);
    std::cout << line << std::endl;
}

}  // namespace

// This process is the *listener*: it hosts the gRPC server and only ever
// receives. The pushing peer connects to it and writes date/time messages.
class BidirServiceImpl final : public BidirService::Service {
public:
    // Note the generated sync signature is ServerReaderWriter<W, R>: the *write*
    // type comes first, so this reads Request (written by the peer) and would
    // write Response.
    Status Stream(ServerContext* context, ServerReaderWriter<Response, Request>* stream) override {
        Log("Push source connected");

        // Blocking read/write loop: one blocking Read per pushed message. Read
        // returns false when the push source half-closes or goes away.
        Request request;
        while (stream->Read(&request)) {
            // The peer pushes unsolicited STATUS updates carrying a date/time string.
            Log("#" + std::to_string(request.id()) + " " + request.payload());
        }

        Log("Push source disconnected");
        if (context->IsCancelled()) {
            return Status(StatusCode::CANCELLED, "Listener shutting down");
        }
        return Status::OK;
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
            // The deadline is what unblocks the in-flight blocking Read; without
            // it Wait() would hang until the push source disconnected on its own.
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
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
