#include <iostream>
#include <memory>
#include <string>
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
    BidirClient(std::shared_ptr<Channel> channel)
        : stub_(BidirService::NewStub(channel)) {}

    void Stream() {
        ClientContext context;
        std::shared_ptr<ClientReaderWriter<Request, Response>> stream(
            stub_->Stream(&context));

        // Thread to read responses
        std::thread reader([stream]() {
            Response response;
            while (stream->Read(&response)) {
                // Placeholder: Extract Opcode and dispatch response
                Opcode op = response.opcode();
                std::cout << "Received response with opcode: " << op 
                          << ", message: " << response.message() << std::endl;
            }
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
            std::cout << "Stream failed: " << status.error_message() << std::endl;
        }
    }

private:
    std::unique_ptr<BidirService::Stub> stub_;
};

int main(int argc, char** argv) {
    BidirClient client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()));
    client.Stream();
    return 0;
}
