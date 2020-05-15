set(proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/sequencer.pb.cc")
set(proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/sequencer.pb.h")
set(grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/sequencer.grpc.pb.cc")
set(grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/sequencer.grpc.pb.h")

get_filename_component(proto "../proto/sequencer/sequencer.proto" ABSOLUTE)
get_filename_component(proto_path "${proto}" PATH)

add_custom_command(
    OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         -I "${proto_path}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${proto}"
    DEPENDS "${proto}"
)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_executable(sequencer
    sequencer.cc
    sequencer_service.cc
    ${proto_srcs}
    ${grpc_srcs}
)

set_property(
    TARGET sequencer 
    PROPERTY CXX_STANDARD 17
)

target_link_libraries(sequencer 
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

