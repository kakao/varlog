export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO := go
GOPATH := $(shell $(GO) env GOPATH)
LDFLAGS :=
GOFLAGS := -race
GCFLAGS := -gcflags=all='-N -l'

PROTOC_HOME := $(BUILD_DIR)/protoc
PROTOC_VERSION := 3.12.2
PROTOC := $(PROTOC_HOME)/bin/protoc
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .

all : proto libvarlog sequencer storage_node sequencer_client metadata_repository

SOLAR_PROTO := proto/varlog
SEQUENCER_PROTO := proto/sequencer
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(SEQUENCER_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)

proto : protoc gogoproto $(PROTO)

SEQUENCER := cmd/sequencer
sequencer : $(SEQUENCER_PROTO) $(SEQUENCER)

STORAGE_NODE := cmd/storage_node
storage_node : $(STORAGE_NODE_PROTO) $(STORAGE_NODE)

LIBVARLOG := pkg/varlog
libvarlog : $(SEQUENCER_PROTO) $(LIBVARLOG)

SEQUENCER_CLIENT := cmd/sequencer_client
sequencer_client : $(SEQUENCER_PROTO) $(SEQUENCER_CLIENT)

METADATA_REPOSITORY := cmd/metadata_repository
metadata_repository : $(METADATA_REPOSITORY_PROTO) $(METADATA_REPOSITORY)

SUBDIRS := $(PROTO) $(SEQUENCER) $(STORAGE_NODE) $(LIBSOLAR) $(SEQUENCER_CLIENT) $(METADATA_REPOSITORY)
subdirs : $(SUBDIRS)

mockgen : pkg/libvarlog/mock/sequencer_mock.go pkg/libvarlog/mock/storage_node_mock.go

pkg/libvarlog/mock/sequencer_mock.go : $(PROTO) proto/sequencer/sequencer.pb.go
	mockgen -source=proto/sequencer/sequencer.pb.go -package mock SequencerServiceClient > pkg/libvarlog/mock/sequencer_mock.go

pkg/libvarlog/mock/storage_node_mock.go : $(PROTO) proto/storage_node/storage_node.pb.go
	mockgen -source=proto/storage_node/storage_node.pb.go -package mock StorageNodeServiceClient > pkg/libvarlog/mock/storage_node_mock.go

$(SUBDIRS) :
	$(MAKE) -C $@

test:
	$(GO) test $(GOFLAGS) $(GCFLAGS) -v ./...

clean :
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

.PHONY: protoc
protoc: $(PROTOC_HOME)/bin/protoc

$(PROTOC_HOME)/bin/protoc:
	PROTOC_HOME=$(PROTOC_HOME) PROTOC_VERSION=$(PROTOC_VERSION) scripts/install_protoc.sh

GOGOPROTO_SRC := $(GOPATH)/src/github.com/gogo/protobuf

.PHONY: gogoproto
gogoproto: $(GOGOPROTO_SRC)

$(GOGOPROTO_SRC)/:
	$(GO) get -u github.com/gogo/protobuf/protoc-gen-gogo
	$(GO) get -u github.com/gogo/protobuf/gogoproto
	$(GO) get -u github.com/gogo/protobuf/proto
	$(GO) get -u github.com/gogo/protobuf/jsonpb

.PHONY : all clean subdirs $(SUBDIRS) mockgen test
