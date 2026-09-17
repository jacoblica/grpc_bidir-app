#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <grpcpp/grpcpp.h>
#include "service.grpc.pb.h"
#include <thread>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderWriter;
using grpc::Status;
using bidir::BidirService;
using bidir::Request;
using bidir::Response;
using bidir::Opcode;

class BidirClient {
public:
    BidirClient(const std::string& address)
        : stub_(BidirService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))),
          address_(address) {}

    void Stream() {
        ClientContext context;
        std::shared_ptr<ClientReaderWriter<Request, Response>> stream(
            stub_->Stream(&context));

        // Thread to read responses
        std::thread reader([this, stream]() {
            std::cout << "Reader thread started for " << address_ << std::endl;
            Response response;
            while (stream->Read(&response)) {
                // Placeholder: Extract Opcode and dispatch response
                Opcode op = response.opcode();
                std::cout << "[" << address_ << "] Received response with opcode: " << op
                          << ", message: " << response.message() << std::endl;
            }
            std::cout << "[" << address_ << "] Stream closed by server" << std::endl;
        });

        // Send requests
        Request request;
        request.set_opcode(Opcode::PING);
        request.set_id(1);
        stream->Write(request);

        request.set_opcode(Opcode::DATA);
        request.set_id(2);
        request.set_payload("Hello Server");
        stream->Write(request);

        stream->WritesDone();
        reader.join();
        Status status = stream->Finish();
        if (!status.ok()) {
            std::cout << "[" << address_ << "] Stream failed: " << status.error_message() << std::endl;
        }
    }

private:
    std::unique_ptr<BidirService::Stub> stub_;
    std::string address_;
};

int main(int argc, char** argv) {
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
        std::cout << "Connecting to server at " << address << std::endl;
        threads.emplace_back([address]() {
            BidirClient client(address);
            client.Stream();
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "All streams finished" << std::endl;
    return 0;
}