cmake_minimum_required(VERSION 3.15)
project(grpc_bidir_app CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
set(_GRPC_GRPCPP gRPC::grpc++)

# Proto generation
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/service.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/service.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/service.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/service.grpc.pb.h")

add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out="${CMAKE_CURRENT_BINARY_DIR}"
           --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
           -I "${CMAKE_CURRENT_SOURCE_DIR}"
           --plugin=protoc-gen-grpc="$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
           "${CMAKE_CURRENT_SOURCE_DIR}/service.proto"
      DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/service.proto")

# Include generated files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Server
add_executable(server server.cpp ${hw_proto_srcs} ${hw_grpc_srcs})
target_link_libraries(server ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF} Threads::Threads)

# Client
add_executable(client client.cpp ${hw_proto_srcs} ${hw_grpc_srcs})
target_link_libraries(client ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF} Threads::Threads)
