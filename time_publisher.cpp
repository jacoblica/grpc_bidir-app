#include <algorithm>
#include <atomic>
#include <chrono>
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

using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using bidir::TimePubService;
using bidir::TimeSubscriber;
using bidir::TimePublisher;
using bidir::Opcode;

namespace {

constexpr auto kPushInterval = std::chrono::seconds(3);
constexpr auto kSleepSlice = std::chrono::milliseconds(200);

std::atomic<bool> g_stop{false};
std::mutex g_log_mu;

void HandleSignal(int) { g_stop.store(true); }

// Each publisher runs on its own thread; keep the shared stdout from interleaving.
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

// Waits out one push interval in short slices: a signal handler cannot touch the
// mutex/condition variable, so the flag is polled while waiting.
void WaitInterval() {
    const auto deadline = std::chrono::steady_clock::now() + kPushInterval;
    while (!g_stop.load() && std::chrono::steady_clock::now() < deadline) {
        const auto now = std::chrono::steady_clock::now();
        std::this_thread::sleep_until(std::min(deadline, now + kSleepSlice));
    }
}

// A blocking Write (or Finish) cannot be interrupted by a flag alone. The
// contexts are registered here so the shutdown thread can cancel them.
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

// This process is the *pusher*: it dials the subscriber and is the only writer
// on the stream, sending a date/time message every 3 seconds.
class TimePubClient {
public:
    explicit TimePubClient(const std::string& address)
        : address_(address),
          stub_(TimePubService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

    void Run() {
        ClientContext context;
        RegisterContext(&context);

        std::unique_ptr<ClientReaderWriter<TimePublisher, TimeSubscriber>> stream = stub_->Stream(&context);
        Log("[" + address_ + "] Connected, pushing date/time every " + std::to_string(kPushInterval.count()) +
            "s; press Ctrl-C to stop.");

        // Blocking read/write loop: one blocking Write per interval. Nothing is
        // ever read, so the subscriber stays the only reader. Write returns
        // false once the subscriber half-closes or goes away.
        TimePublisher message;
        int32_t pushed = 0;
        bool write_failed = false;
        while (!g_stop.load()) {
            WaitInterval();
            if (g_stop.load()) break;

            ++pushed;
            message.set_opcode(Opcode::STATUS);
            message.set_id(pushed);
            message.set_message(FormatTimestamp());
            message.set_success(true);
            message.set_result(0.0F);
            if (!stream->Write(message)) {
                write_failed = true;
                break;
            }
        }

        stream->WritesDone();
        Status status = stream->Finish();
        UnregisterContext(&context);
        if (write_failed || !status.ok()) {
            Log("[" + address_ + "] Stream finished: " + status.error_message());
        } else {
            Log("[" + address_ + "] Stream finished: OK");
        }
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
            HandleSignal(signal_number);
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
            TimePubClient publisher(address);
            publisher.Run();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}
