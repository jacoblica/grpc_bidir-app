#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::Channel;
using grpc::ChannelArguments;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using bidir::TimePubService;
using bidir::TimeSubscriber;
using bidir::TimePublisher;
using bidir::Opcode;

namespace {

constexpr auto K_PUSH_INTERVAL = std::chrono::seconds(3);
constexpr auto K_SLEEP_SLICE = std::chrono::milliseconds(200);

// One connection attempt may not stall the retry loop forever, so a dial that
// has not reached READY within this budget counts as a failed attempt.
constexpr auto K_CONNECT_TIMEOUT = std::chrono::seconds(2);
constexpr auto K_CONNECT_POLL_SLICE = std::chrono::milliseconds(200);

// Retry pacing: the wait grows with every consecutive failed attempt, is capped
// at kRetryMax and is jittered so that several publishers which lost the same
// subscriber do not all come back on the same tick.
constexpr auto K_RETRY_INITIAL = std::chrono::milliseconds(1000);
constexpr auto K_RETRY_MAX = std::chrono::milliseconds(15000);
constexpr int K_MAX_BACKOFF_SHIFT = 4;

// Keepalive bounds how long a silently dropped network stays unnoticed: without
// it the blocking Write below simply waits on a dead TCP connection until the
// OS gives up (minutes), instead of failing the stream so the retry loop can
// reconnect.
//
// This interval cannot be made aggressive. gRPC resets the keepalive timer only
// when *incoming* bytes arrive, and this publisher never receives anything (the
// subscriber never writes), so pushing date/time every kPushInterval does not
// postpone anything: a ping goes out every kKeepAliveTimeMs regardless. A ping
// every few seconds also collides with the ping/BDP traffic our own 3s pushes
// generate, and a stock gRPC server keeps its own ping-strike policy
// (grpc.http2.max_ping_strikes, 2 by default). Together those turned a
// perfectly healthy stream into "keepalive watchdog timeout" every ~15s.
// Keeping the ping cadence an order of magnitude above the push cadence makes
// it reliable; the price is the detection budget below.
constexpr int K_KEEP_ALIVE_TIME_MS = 30000;
constexpr int K_KEEP_ALIVE_TIMEOUT_MS = 10000;

std::atomic<bool> g_stop{false};
std::mutex g_log_mu;

void handleSignal(int) { g_stop.store(true); }

// Each publisher runs on its own thread; keep the shared stdout from interleaving.
void log(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_log_mu);
    std::cout << line << std::endl;
}

std::string formatTimestamp() {
    const std::time_t NOW = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm_buf{};
    localtime_r(&NOW, &tm_buf);
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

// Waits out `duration` in short slices: a signal handler cannot touch the
// mutex/condition variable, so the flag is polled while waiting. Serves both as
// the push cadence and as the retry backoff, and returns early on Ctrl-C.
void waitInterruptible(std::chrono::milliseconds duration) {
    const auto DEADLINE = std::chrono::steady_clock::now() + duration;
    while (!g_stop.load()) {
        const auto NOW = std::chrono::steady_clock::now();
        if (NOW >= DEADLINE) return;
        std::this_thread::sleep_until(std::min(DEADLINE, NOW + K_SLEEP_SLICE));
    }
}

// Jittered exponential backoff. The generator is per thread because every
// publisher retries on its own thread and rand() is not the place for that.
std::chrono::milliseconds backoffDelay(int consecutive_failures) {
    thread_local std::mt19937 s_generator(
        static_cast<unsigned int>(std::chrono::steady_clock::now().time_since_epoch().count()));
    const int SHIFT = std::min(consecutive_failures, K_MAX_BACKOFF_SHIFT);
    const auto CEILING = std::min(K_RETRY_INITIAL * (1 << SHIFT), K_RETRY_MAX);
    std::uniform_int_distribution<int> jitter(0, static_cast<int>(CEILING.count()) - 1);
    return std::chrono::milliseconds(jitter(s_generator));
}

// A fresh channel per attempt: a new subscriber process, a restarted network
// interface or a recovered link is then redialled immediately instead of
// waiting out the reconnect backoff of a channel that already gave up.
std::shared_ptr<Channel> createChannel(const std::string& address) {
    ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, K_KEEP_ALIVE_TIME_MS);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, K_KEEP_ALIVE_TIMEOUT_MS);
    // 0 = unlimited pings between data frames, so a peer that does count them
    // never answers our heartbeat with a GOAWAY.
    args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, static_cast<int>(K_RETRY_INITIAL.count()));
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, static_cast<int>(K_RETRY_INITIAL.count()));
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, static_cast<int>(K_RETRY_MAX.count()));
    return grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
}

const char* connectivityName(grpc_connectivity_state state) {
    switch (state) {
        case GRPC_CHANNEL_IDLE:
            return "IDLE";
        case GRPC_CHANNEL_CONNECTING:
            return "CONNECTING";
        case GRPC_CHANNEL_READY:
            return "READY";
        case GRPC_CHANNEL_TRANSIENT_FAILURE:
            return "TRANSIENT_FAILURE";
        case GRPC_CHANNEL_SHUTDOWN:
            return "SHUTDOWN";
        default:
            return "UNKNOWN";
    }
}

// A blocking Write (or Finish) cannot be interrupted by a flag alone. The
// contexts are registered here so the shutdown thread can cancel them.
std::mutex g_registry_mu;
std::vector<ClientContext*> g_contexts;

void registerContext(ClientContext* context) {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    g_contexts.push_back(context);
}

void unregisterContext(ClientContext* context) {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    g_contexts.erase(std::remove(g_contexts.begin(), g_contexts.end(), context), g_contexts.end());
}

void cancelAllContexts() {
    std::lock_guard<std::mutex> lk(g_registry_mu);
    for (ClientContext* context : g_contexts) {
        context->TryCancel();
    }
}

}  // namespace

// This process is the *pusher*: it dials the subscriber and is the only writer
// on the stream, sending a date/time message every 3 seconds.
//
// It is expected to outlive any single connection, so `Run` keeps retrying
// instead of returning: a subscriber that is not up yet, a stream that breaks and
// a network that disappears all just restart the retry loop, and the date/time
// pushes resume on their own once the subscriber is reachable again. The push
// counter is kept across attempts, so the subscriber can tell that the resumed
// stream is the same publisher and not a fresh one.
class TimePubClient {
public:
    explicit TimePubClient(std::string address) : address_(std::move(address)) {}

    void run() {
        int consecutive_failures = 0;
        while (!g_stop.load()) {
            const bool ESTABLISHED = pushOnce(consecutive_failures);
            if (g_stop.load()) break;

            // A stream that came up clears the penalty; only attempts that never
            // reached the subscriber make the backoff grow.
            consecutive_failures = ESTABLISHED ? 0 : consecutive_failures + 1;

            const auto DELAY = backoffDelay(consecutive_failures);
            log("[" + address_ + "] Retrying in " + std::to_string(DELAY.count()) + "ms");
            waitInterruptible(DELAY);
        }
    }

private:
    // Blocks until the transport reports READY, so "subscriber unreachable" is
    // detected here instead of surfacing later as a failed Write. Returns false
    // on timeout or Ctrl-C, which the caller treats as a failed attempt.
    bool waitForChannelReady(Channel* channel) {
        const auto DEADLINE = std::chrono::system_clock::now() + K_CONNECT_TIMEOUT;
        grpc_connectivity_state state = channel->GetState(/*try_to_connect=*/true);
        while (state != GRPC_CHANNEL_READY) {
            if (g_stop.load() || std::chrono::system_clock::now() >= DEADLINE) {
                if (!g_stop.load()) {
                    log("[" + address_ + "] " + address_ + " not reachable, channel state " +
                        connectivityName(state));
                }
                return false;
            }
            // No logging in here on purpose: a channel that cannot be reached
            // cycles IDLE/CONNECTING/TRANSIENT_FAILURE several times inside one
            // attempt, and only the verdict above is worth printing.
            channel->WaitForStateChange(state, std::min(DEADLINE, std::chrono::system_clock::now() + K_CONNECT_POLL_SLICE));
            state = channel->GetState(/*try_to_connect=*/true);
        }
        return true;
    }

    // One complete attempt: dial, wait for the transport, then push every
    // kPushInterval until the subscriber half-closes, the connection dies or
    // Ctrl-C arrives. Returns whether a stream was established at all, so that
    // Run() can tell a first connect apart from a failed one and pace retries.
    bool pushOnce(int consecutive_failures) {
        std::shared_ptr<Channel> channel = createChannel(address_);
        if (!waitForChannelReady(channel.get())) {
            return false;
        }

        if (pushed_ == 0) {
            log("[" + address_ + "] Connected, pushing date/time every " + std::to_string(K_PUSH_INTERVAL.count()) +
                "s; press Ctrl-C to stop.");
        } else if (consecutive_failures == 0) {
            log("[" + address_ + "] Connection recovered, resuming date/time at #" + std::to_string(pushed_ + 1));
        } else {
            log("[" + address_ + "] Connection recovered after " + std::to_string(consecutive_failures) +
                " failed attempt(s), resuming date/time at #" + std::to_string(pushed_ + 1));
        }

        std::unique_ptr<TimePubService::Stub> stub = TimePubService::NewStub(channel);
        ClientContext context;
        registerContext(&context);

        std::unique_ptr<ClientReaderWriter<TimePublisher, TimeSubscriber>> stream = stub->Stream(&context);

        // Blocking read/write loop: one blocking Write per interval. Nothing is
        // ever read, so the subscriber stays the only reader. Write returns
        // false once the subscriber half-closes or goes away.
        TimePublisher message;
        bool write_failed = false;
        while (!g_stop.load()) {
            waitInterruptible(K_PUSH_INTERVAL);
            if (g_stop.load()) break;

            ++pushed_;
            message.set_opcode(Opcode::STATUS);
            message.set_id(pushed_);
            message.set_message(formatTimestamp());
            message.set_success(true);
            message.set_result(0.0F);
            if (!stream->Write(message)) {
                write_failed = true;
                break;
            }
        }

        stream->WritesDone();
        Status status = stream->Finish();
        unregisterContext(&context);

        if (g_stop.load()) {
            log("[" + address_ + "] Stream finished: " + status.error_message());
        } else {
            // Either the write failed (peer gone, or keepalive reaped a network
            // that went silent for kKeepAliveTimeMs + kKeepAliveTimeoutMs) or the
            // subscriber half-closed between two pushes. Both mean the same thing
            // here: the stream is gone, reconnect.
            log("[" + address_ + "] " + (write_failed ? "Connection lost" : "Stream closed") + ": " +
                std::to_string(status.error_code()) + " " + status.error_message());
        }
        return true;
    }

    std::string address_;
    int32_t pushed_ = 0;
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
            log("Received signal " + std::to_string(signal_number) + ", cancelling streams");
            handleSignal(signal_number);
            cancelAllContexts();
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
            publisher.run();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}
