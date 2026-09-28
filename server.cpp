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
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

namespace {

constexpr auto kPushInterval = std::chrono::seconds(3);
constexpr auto kSleepSlice = std::chrono::milliseconds(200);

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

// Waits out one push interval in short slices: a signal handler cannot touch the
// mutex/condition variable, so the flag is polled while waiting.
void WaitInterval() {
    const auto deadline = std::chrono::steady_clock::now() + kPushInterval;
    while (!g_stop.load() && std::chrono::steady_clock::now() < deadline) {
        const auto now = std::chrono::steady_clock::now();
        std::this_thread::sleep_until(std::min(deadline, now + kSleepSlice));
    }
}

}  // namespace

// One pushing connection per target address.
class PushClient {
public:
    explicit PushClient(const std::string& address)
        : address_(address),
          stub_(BidirService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))) {}

    void Run() {
        ClientContext context;
        std::unique_ptr<ClientReaderWriter<Request, Response>> stream = stub_->Stream(&context);
        Log("[" + address_ + "] Connected, pushing date/time every " + std::to_string(kPushInterval.count()) +
            "s; press Ctrl-C to stop.");

        // Blocking read/write loop: one blocking Write per interval. Nothing is
        // ever read, so the listener stays the only reader.
        Request request;
        int32_t pushed = 0;
        bool write_failed = false;
        while (!g_stop.load()) {
            WaitInterval();
            if (g_stop.load()) break;

            ++pushed;
            request.set_opcode(Opcode::STATUS);
            request.set_id(pushed);
            request.set_payload(FormatTimestamp());
            request.set_active(true);
            request.set_value(0.0F);
            if (!stream->Write(request)) {
                // Listener went away; Write reports the broken stream.
                write_failed = true;
                break;
            }
        }

        stream->WritesDone();
        Status status = stream->Finish();
        if (write_failed || !status.ok()) {
            Log("[" + address_ + "] Stream finished: " + status.error_message());
        } else {
            Log("[" + address_ + "] Stream finished: OK");
        }
    }

private:
    std::string address_;
    std::unique_ptr<BidirService::Stub> stub_;
};

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
