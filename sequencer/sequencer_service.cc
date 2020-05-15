#include <iostream>
#include <grpcpp/grpcpp.h>

#include "sequencer.grpc.pb.h"
#include "sequencer.h"

namespace solar {

class SequencerServiceImpl : public SequencerService::Service {
 public:
  grpc::Status Next(
      grpc::ServerContext *context,
      grpc::ServerReaderWriter<SequencerResponse, SequencerRequest> *stream)
      override {
    SequencerRequest request;
    while (stream->Read(&request)) {
      SequencerResponse response;
      response.set_glsn(sequencer_.Next());
      stream->Write(response);
    }
    return grpc::Status::OK;
  }

 private:
  Sequencer sequencer_;
};

}  // namespace solar

int main() {
  std::cout << "sequencer"
            << "\n";

  solar::SequencerServiceImpl service;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("0.0.0.0:9091", grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  server->Wait();
  return 0;
}
