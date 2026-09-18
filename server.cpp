#include <iostream>
#include <memory>
#include <mutex>
#include <string>
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

class StreamReactor final : public ServerBidiReactor<Request, Response> {
public:
    StreamReactor() {
        // Kick off the read/write callback loop by requesting the first message.
        StartRead(&request_);
    }

    void OnReadDone(bool ok) override {
        if (finished_) return;
        if (!ok) {
            // Client closed its write side (WritesDone) or the call ended.
            Finish(Status::OK);
            finished_ = true;
            return;
        }

        // Placeholder: Extract Opcode and dispatch
        Opcode op = request_.opcode();
        std::cout << "Received request with opcode: " << op << std::endl;

        response_.set_opcode(op);
        response_.set_id(request_.id());

        switch (op) {
            case bidir::Opcode::PING:
                // TODO: Handle PING
                response_.set_message("PONG");
                break;
            case bidir::Opcode::DATA:
                // TODO: Handle DATA
                response_.set_message("DATA RECEIVED");
                break;
            default:
                response_.set_message("UNKNOWN OPCODE");
                break;
        }

        StartWrite(&response_);
    }

    void OnWriteDone(bool ok) override {
        if (finished_) return;
        if (!ok) {
            Finish(Status(grpc::StatusCode::INTERNAL, "Write failed"));
            finished_ = true;
            return;
        }
        StartRead(&request_);
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
    Request request_;
    Response response_;
    bool finished_ = false;
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
    std::cout << "Server listening on " << listen_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunServer(address);
    return 0;
}