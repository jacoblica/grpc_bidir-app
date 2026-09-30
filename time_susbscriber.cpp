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
using bidir::TimePubService;
using bidir::TimeSubscriber;
using bidir::TimePublisher;
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
// receives. The publishers dial in and write date/time messages.
class TimePubServiceImpl final : public TimePubService::Service {
public:
    // Note the generated sync signature is ServerReaderWriter<W, R>: the *write*
    // type comes first, so this reads TimePublisher and would write TimeSubscriber.
    Status Stream(ServerContext* context, ServerReaderWriter<TimeSubscriber, TimePublisher>* stream) override {
        const std::string peer = context->peer();
        Log("[" + peer + "] Time publisher connected");

        // Blocking read/write loop: one blocking Read per pushed message. Read
        // returns false when the publisher half-closes or goes away. The
        // publishers never write, so this simply parks until they leave.
        TimePublisher message;
        while (stream->Read(&message)) {
            Log("[" + peer + "] #" + std::to_string(message.id()) + " " + message.message());
        }

        Log("[" + peer + "] Time publisher disconnected");
        if (context->IsCancelled()) {
            return Status(StatusCode::CANCELLED, "Subscriber shutting down");
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

    TimePubServiceImpl service;

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
            Log("Received signal " + std::to_string(signal_number) + ", shutting down");
            // The deadline is what unblocks the in-flight blocking Read; without
            // it Wait() would hang until the publisher disconnected on its own.
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
        }
    });

    std::cout << "Listening on " << listen_address
              << "; receiving date/time from each publisher. Ctrl-C to stop." << std::endl;
    server->Wait();
    signal_thread.join();
    std::cout << "Subscriber stopped" << std::endl;
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunListener(address);
    return 0;
}
