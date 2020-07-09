export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO := go
GOPATH := $(shell $(GO) env GOPATH)
LDFLAGS :=
GOFLAGS := -race
GCFLAGS := -gcflags=all='-N -l'

PROTOC_VERSION := 3.12.3
PROTOC_HOME := $(BUILD_DIR)/protoc
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .

TEST_COUNT := 1
TEST_FLAGS := -timeout 20m -count $(TEST_COUNT) -p 1

ifneq ($(TEST_PARALLEL),)
	TEST_FLAGS := $(TEST_FLAGS) -parallel $(TEST_PARALLEL)
endif

TEST_FAILFAST := 0
ifeq ($(TEST_FAILFAST),1)
	TEST_FLAGS := $(TEST_FLAGS) -failfast
endif

TEST_VERBOSE := 1
ifeq ($(TEST_VERBOSE),1)
	TEST_FLAGS := $(TEST_FLAGS) -v
endif

TEST_DIRS := $(sort $(dir $(shell find . -name '*_test.go')))

all : proto libvarlog storage_node metadata_repository

SOLAR_PROTO := proto/varlog
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)

proto : check_protoc gogoproto $(PROTO)

STORAGE_NODE := cmd/storage_node
storage_node : $(STORAGE_NODE_PROTO) $(STORAGE_NODE)

LIBVARLOG := pkg/varlog
libvarlog : $(LIBVARLOG)

METADATA_REPOSITORY := cmd/metadata_repository
metadata_repository : $(METADATA_REPOSITORY_PROTO) $(METADATA_REPOSITORY)

SUBDIRS := $(PROTO) $(STORAGE_NODE) $(LIBSOLAR) $(METADATA_REPOSITORY)
subdirs : $(SUBDIRS)

$(SUBDIRS) :
	$(MAKE) -C $@

mockgen: pkg/varlog/mock/storage_node_mock.go internal/storage/storage_mock.go

internal/storage/storage_mock.go: internal/storage/storage.go
	mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storage 
		-package storage \
		-source ./internal/storage/storage.go \
		> internal/storage/storage_mock.go

internal/storage/log_stream_executor_mock.go: internal/storage/log_stream_executor.go
	mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storage \
		-package storage \
		-source internal/storage/log_stream_executor.go \
		> internal/storage/log_stream_executor_mock.go

internal/storage/replicator_mock.go: internal/storage/replicator.go
	mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storage \
		-package storage \
		-source internal/storage/replicator.go \
		> internal/storage/replicator_mock.go

internal/storage/replicator_client_mock.go: internal/storage/replicator_client.go
	mockgen -self_package github.daumkakao.com/varlog/varlog/internal/storage \
		-package storage \
		-source internal/storage/replicator_client.go \
		> internal/storage/replicator_client_mock.go

proto/storage_node/mock/replicator_mock.go: $(PROTO) proto/storage_node/replicator.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		github.daumkakao.com/varlog/varlog/proto/storage_node \
		ReplicatorServiceClient,ReplicatorServiceServer,ReplicatorService_ReplicateClient,ReplicatorService_ReplicateServer \
		> proto/storage_node/mock/replicator_mock.go

pkg/varlog/mock/storage_node_mock.go: $(PROTO) proto/storage_node/storage_node.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		github.daumkakao.com/varlog/varlog/proto/storage_node \
		StorageNodeServiceClient,StorageNodeServiceServer,StorageNodeService_SubscribeClient,StorageNodeService_SubscribeServer \
		> pkg/varlog/mock/storage_node_mock.go

test:
	$(GO) test $(GOFLAGS) $(GCFLAGS) $(TEST_FLAGS) ./...

clean :
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

.PHONY: check_protoc
check_protoc:
ifneq ($(wildcard $(PROTOC_HOME)/bin/protoc),)
	$(info Installed protoc: $(shell $(PROTOC_HOME)/bin/protoc --version))
else
ifneq ($(shell which protoc),)
	$(info Installed protoc: $(shell protoc --version))
else
	$(error Install protoc. You can use '"make protoc"'.)
endif
endif

.PHONY: protoc
protoc: $(PROTOC_HOME)

$(PROTOC_HOME):
	PROTOC_HOME=$(PROTOC_HOME) PROTOC_VERSION=$(PROTOC_VERSION) scripts/install_protoc.sh

GOGOPROTO_SRC := $(GOPATH)/src/github.com/gogo/protobuf

.PHONY: gogoproto
gogoproto: $(GOGOPROTO_SRC)

$(GOGOPROTO_SRC):
	$(GO) get -u github.com/gogo/protobuf/protoc-gen-gogo
	$(GO) get -u github.com/gogo/protobuf/gogoproto
	$(GO) get -u github.com/gogo/protobuf/proto
	$(GO) get -u github.com/gogo/protobuf/jsonpb

.PHONY : all clean subdirs $(SUBDIRS) mockgen test
