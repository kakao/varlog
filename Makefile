export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO_VERSION := 1.14.4
GO_HOME := $(HOME)/go

PROTOBUF_VERSION := 3.12.2
PROTOBUF_HOME := $(BUILD_DIR)/protobuf

GO := go
LDFLAGS :=
CFLAGS := -gcflags=all="-N -l" -race
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .

GOPATH := $(shell $(GO) env GOPATH)
ifeq ($(GOPATH), )
	GOPATH := $(HOME)/gopath
endif

all : proto libvarlog sequencer storage_node sequencer_client

SOLAR_PROTO := proto/varlog
SEQUENCER_PROTO := proto/sequencer
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(SEQUENCER_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)
proto : $(PROTO)

SEQUENCER := cmd/sequencer
sequencer : $(SEQUENCER_PROTO) $(SEQUENCER)

STORAGE_NODE := cmd/storage_node
storage_node : $(STORAGE_NODE_PROTO) $(STORAGE_NODE)

LIBVARLOG := pkg/varlog
libvarlog : $(SEQUENCER_PROTO) $(LIBVARLOG)

SEQUENCER_CLIENT := cmd/sequencer_client
sequencer_client : $(SEQUENCER_PROTO) $(SEQUENCER_CLIENT)

SUBDIRS := $(PROTO) $(SEQUENCER) $(STORAGE_NODE) $(LIBVARLOG) $(SEQUENCER_CLIENT)
subdirs : $(SUBDIRS)

mockgen : pkg/libvarlog/mock/sequencer_mock.go pkg/libvarlog/mock/storage_node_mock.go

pkg/libvarlog/mock/sequencer_mock.go : $(PROTO) proto/sequencer/sequencer.pb.go
	mockgen -source=proto/sequencer/sequencer.pb.go -package mock SequencerServiceClient > pkg/libvarlog/mock/sequencer_mock.go

pkg/libvarlog/mock/storage_node_mock.go : $(PROTO) proto/storage_node/storage_node.pb.go
	mockgen -source=proto/storage_node/storage_node.pb.go -package mock StorageNodeServiceClient > pkg/libvarlog/mock/storage_node_mock.go

$(SUBDIRS) :
	$(MAKE) -C $@

test:
	PATH=$$PATH:$(GO_HOME)/bin GOPATH=$(GOPATH) $(GO) test -v ./...

clean :
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

golang:
	GO_VERSION=$(GO_VERSION) GO_HOME=$(GO_HOME) scripts/golang.sh

protobuf:
	PROTOBUF_VERSION=$(PROTOBUF_VERSION) PROTOBUF_HOME=$(PROTOBUF_HOME) scripts/protobuf.sh

gogoproto:
	PATH=$$PATH:$(GO_HOME)/bin GOPATH=$(GOPATH) $(GO) get github.com/gogo/protobuf/proto
	PATH=$$PATH:$(GO_HOME)/bin GOPATH=$(GOPATH) $(GO) get github.com/gogo/protobuf/jsonpb
	PATH=$$PATH:$(GO_HOME)/bin GOPATH=$(GOPATH) $(GO) get github.com/gogo/protobuf/protoc-gen-gogo
	PATH=$$PATH:$(GO_HOME)/bin GOPATH=$(GOPATH) $(GO) get github.com/gogo/protobuf/gogoproto

.PHONY : all clean subdirs $(SUBDIRS) mockgen golang protobuf gogoproto test
