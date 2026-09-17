#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

class BidirServiceImpl final : public BidirService::Service {
    Status Stream(ServerContext* context, ServerReaderWriter<Response, Request>* stream) override {
        Request request;
        while (stream->Read(&request)) {
            // Placeholder: Extract Opcode and dispatch
            Opcode op = request.opcode();
            std::cout << "Received request with opcode: " << op << std::endl;

            Response response;
            response.set_opcode(op);
            response.set_id(request.id());

            switch (op) {
                case bidir::Opcode::PING:
                    // TODO: Handle PING
                    response.set_message("PONG");
                    break;
                case bidir::Opcode::DATA:
                    // TODO: Handle DATA
                    response.set_message("DATA RECEIVED");
                    break;
                default:
                    response.set_message("UNKNOWN OPCODE");
                    break;
            }

            stream->Write(response);
        }
        return Status::OK;
    }
};

void RunServer(const std::string& listen_address) {
    std::string server_address = listen_address;
    BidirServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "0.0.0.0:50051";
    RunServer(address);
    return 0;
}
