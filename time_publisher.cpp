#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sstream>
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

constexpr auto kPushInterval = std::chrono::seconds(3);
constexpr auto kSleepSlice = std::chrono::milliseconds(200);

std::atomic<bool> g_stop{false};
std::mutex g_log_mu;

void HandleSignal(int) { g_stop.store(true); }

// Each stream is handled on its own gRPC sync thread; keep stdout readable.
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

// This process is the *listener* and the only writer on the stream: it hosts the
// gRPC server, and every connected client receives a date/time message every
// 3 seconds.
class TimePubServiceImpl final : public TimePubService::Service {
public:
    // Note the generated sync signature is ServerReaderWriter<W, R>: the *write*
    // type comes first, so this writes TimePublisher and would read TimeSubscriber.
    Status Stream(ServerContext* context, ServerReaderWriter<TimePublisher, TimeSubscriber>* stream) override {
        Log("Client connected");

        // Blocking read/write loop: one blocking Write per interval. Write returns
        // false once the client half-closes or goes away, which is how a
        // disconnect is noticed.
        TimePublisher response;
        int32_t pushed = 0;
        while (!g_stop.load()) {
            WaitInterval();
            if (g_stop.load()) break;

            ++pushed;
            response.set_opcode(Opcode::STATUS);
            response.set_id(pushed);
            response.set_message(FormatTimestamp());
            response.set_success(true);
            response.set_result(0.0F);
            if (!stream->Write(response)) {
                break;
            }
        }

        Log("Client disconnected");
        if (context->IsCancelled()) {
            return Status(StatusCode::CANCELLED, "Server shutting down");
        }
        return Status::OK;
    }
};

void RunServer(const std::string& listen_address) {
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
            // Flag the write loops, then give in-flight RPCs a deadline so
            // Wait() cannot be held open by a handler.
            HandleSignal(signal_number);
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(1));
        }
    });

    std::cout << "Listening on " << listen_address
              << "; pushing date/time every " << kPushInterval.count() << "s to each client. "
              << "Ctrl-C to stop." << std::endl;
    server->Wait();
    signal_thread.join();
    std::cout << "Server stopped" << std::endl;
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunServer(address);
    return 0;
}
