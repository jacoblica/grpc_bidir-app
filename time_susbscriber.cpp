#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using bidir::TimePubService;
using bidir::TimeSubscriber;
using bidir::TimePublisher;
using bidir::Opcode;

namespace {

std::mutex g_log_mu;

void Log(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_log_mu);
    std::cout << line << std::endl;
}

// Every connection blocks in a synchronous Read, so a signal flag alone cannot
// stop it. The contexts are registered here so the shutdown thread can cancel
// them, which unblocks the pending Read.
std::mutex g_registry_mu;
std::vector<ClientContext*> g_contexts;

void RegisterContext(ClientContext* context) {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    g_contexts.push_back(context);
}

void UnregisterContext(ClientContext* context) {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    g_contexts.erase(std::remove(g_contexts.begin(), g_contexts.end(), context), g_contexts.end());
}

void CancelAllContexts() {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    for (ClientContext* context : g_contexts) {
        context->TryCancel();
    }
}

}  // namespace

// This process is the *connector*: it dials one or more servers and only ever
// receives; the server pushes a date/time message every 3 seconds.
class BidirClient {
public:
    explicit BidirClient(const std::string& address)
        : address_(address),
          stub_(TimePubService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

    void Run() {
        ClientContext context;
        RegisterContext(&context);

        std::unique_ptr<ClientReaderWriter<TimeSubscriber, TimePublisher>> stream = stub_->Stream(&context);
        Log("[" + address_ + "] Connected, receiving date/time; press Ctrl-C to stop.");

        // Blocking read/write loop: one blocking Read per pushed message. Read
        // returns false when the server ends the stream or the call is cancelled.
        TimePublisher response;
        while (stream->Read(&response)) {
            Log("[" + address_ + "] #" + std::to_string(response.id()) + " " + response.message());
        }

        Status status = stream->Finish();
        UnregisterContext(&context);
        Log("[" + address_ + "] Stream finished: " + (status.ok() ? "OK" : status.error_message()));
    }

private:
    std::string address_;
    std::unique_ptr<TimePubService::Stub> stub_;
};

int main(int argc, char** argv) {
    // Block the signals so the dedicated thread below can consume them safely.
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);

    std::thread signal_thread([&signals]() {
        int signal_number = 0;
        if (sigwait(&signals, &signal_number) == 0) {
            Log("Received signal " + std::to_string(signal_number) + ", cancelling streams");
            CancelAllContexts();
        }
    });
    // Nothing to join: the thread stays parked in sigwait until the process exits.
    signal_thread.detach();

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
            BidirClient client(address);
            client.Run();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}
