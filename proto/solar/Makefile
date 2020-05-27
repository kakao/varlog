SRC := metadata.proto
# TARGET_PATH := $(MAKEFILE_DIR)/pkg/solar
# TARGET := $(TARGET_PATH)/$(SRC:.proto=.pb.go)
TARGET := $(SRC:.proto=.pb.go)

$(TARGET): $(SRC)
	$(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. \
		$(SRC)

clean:
	$(RM) $(TARGET)

