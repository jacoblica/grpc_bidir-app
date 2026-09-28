#include <chrono>
#include <condition_variable>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
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

namespace {

constexpr auto kPushInterval = std::chrono::seconds(3);

std::string FormatTimestamp() {
    const std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm tm_buf{};
    localtime_r(&now, &tm_buf);
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

}  // namespace

class StreamReactor final : public ServerBidiReactor<Request, Response> {
public:
    StreamReactor() {
        // Arm a read purely to observe the client half-closing or going away;
        // the client is not expected to send anything.
        StartRead(&request_);
        // Per-connection ticker that pushes the clock on a fixed cadence.
        ticker_ = std::thread([this] { TickerLoop(); });
    }

    void OnReadDone(bool ok) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        if (!ok) {
            // Client closed its write side or the call ended.
            stopped_ = true;
            cv_.notify_all();
            Finish(Status::OK);
            return;
        }

        // Nothing to dispatch: the server is the only writer on this stream.
        StartRead(&request_);
    }

    void OnWriteDone(bool ok) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        write_in_progress_ = false;
        if (!ok) {
            stopped_ = true;
            cv_.notify_all();
            Finish(Status(grpc::StatusCode::INTERNAL, "Write failed"));
            return;
        }
        // A tick landed while this write was in flight; push it right away.
        if (write_pending_) {
            write_pending_ = false;
            StartPushLocked();
        }
    }

    void OnCancel() override {
        std::lock_guard<std::mutex> lk(mu_);
        if (stopped_) return;
        stopped_ = true;
        cv_.notify_all();
        Finish(Status(grpc::StatusCode::CANCELLED, "Call cancelled"));
    }

    void OnDone() override {
        // RPC is fully complete; stop the ticker before reclaiming the reactor.
        {
            std::lock_guard<std::mutex> lk(mu_);
            stopped_ = true;
        }
        cv_.notify_all();
        if (ticker_.joinable()) ticker_.join();
        delete this;
    }

private:
    void TickerLoop() {
        std::unique_lock<std::mutex> lk(mu_);
        while (!cv_.wait_for(lk, kPushInterval, [this] { return stopped_; })) {
            if (write_in_progress_) {
                // Never touch response_ while it is owned by an in-flight write.
                write_pending_ = true;
                continue;
            }
            StartPushLocked();
        }
    }

    // mu_ must be held: response_ is only mutated here and in StartPushLocked.
    void StartPushLocked() {
        ++pushed_;
        response_.set_opcode(Opcode::STATUS);
        response_.set_id(static_cast<int32_t>(pushed_));
        response_.set_message(FormatTimestamp());
        response_.set_success(true);
        response_.set_result(0.0F);
        write_in_progress_ = true;
        StartWrite(&response_);
    }

    Request request_;
    Response response_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread ticker_;
    bool stopped_ = false;
    bool write_in_progress_ = false;
    bool write_pending_ = false;
    int32_t pushed_ = 0;
};

class BidirServiceImpl final : public BidirService::CallbackService {
    ServerBidiReactor<Request, Response>* Stream(CallbackServerContext* context) override {
        return new StreamReactor();
    }
};

void RunServer(const std::string& listen_address) {
    BidirServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << listen_address
              << " (pushing date/time every " << kPushInterval.count() << "s)" << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunServer(address);
    return 0;
}
