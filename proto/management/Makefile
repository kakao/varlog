SRC := vms.proto
TARGET := $(SRC:.proto=.pb.go)

$(TARGET): $(SRC)
	for src in $(SRC) ; do \
		PATH=$(PROTOC_HOME)/bin:$$PATH $(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. $$src ; \
	done

clean:
	$(RM) $(TARGET)

