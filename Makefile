export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO := go
GOPATH := $(shell $(GO) env GOPATH)
LDFLAGS :=
GOFLAGS := -race
GCFLAGS := -gcflags=all='-N -l'

PKG_SRCS := $(abspath $(shell find . -name '*.go'))
INTERNAL_SRCS := $(abspath $(shell find . -name '*.go'))

PROTOC_VERSION := 3.12.3
PROTOC_HOME := $(BUILD_DIR)/protoc
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .

TEST_COUNT := 1
TEST_FLAGS := -count $(TEST_COUNT) -p 1

ifneq ($(TEST_TIMEOUT),)
	TEST_FLAGS := $(TEST_FLAGS) -timeout $(TEST_TIMEOUT)
endif

ifneq ($(TEST_PARALLEL),)
	TEST_FLAGS := $(TEST_FLAGS) -parallel $(TEST_PARALLEL)
endif

TEST_COVERAGE := 0
ifeq ($(TEST_COVERAGE),1)
	TEST_FLAGS := $(TEST_FLAGS) -coverprofile=$(BUILD_DIR)/reports/coverage.out
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

all : proto libvarlog storagenode metadata_repository

SOLAR_PROTO := proto/varlog
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)

proto : check_protoc gogoproto $(PROTO)

STORAGE_NODE := cmd/storagenode
storagenode : $(STORAGE_NODE_PROTO) $(STORAGE_NODE)

LIBVARLOG := pkg/varlog
libvarlog : $(LIBVARLOG)

METADATA_REPOSITORY := cmd/metadata_repository
metadata_repository : $(METADATA_REPOSITORY_PROTO) $(METADATA_REPOSITORY)

SUBDIRS := $(PROTO) $(STORAGE_NODE) $(LIBSOLAR) $(METADATA_REPOSITORY)
subdirs : $(SUBDIRS)

$(SUBDIRS) :
	$(MAKE) -C $@

mockgen: \
	internal/storage/storage_node_mock.go \
	internal/storage/storage_mock.go \
	internal/storage/log_stream_executor_mock.go \
	internal/storage/log_stream_reporter_mock.go \
	internal/storage/log_stream_reporter_client_mock.go \
	internal/storage/replicator_mock.go \
	internal/storage/replicator_client_mock.go \
	proto/storage_node/mock/replicator_mock.go \
	proto/storage_node/mock/log_io_mock.go \
	proto/storage_node/mock/log_stream_reporter_mock.go \
	proto/storage_node/mock/management_mock.go

internal/storage/storage_node_mock.go: internal/storage/storage_node.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/storage_mock.go: internal/storage/storage.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/log_stream_executor_mock.go: internal/storage/log_stream_executor.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/log_stream_reporter_mock.go: internal/storage/log_stream_reporter.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/log_stream_reporter_client_mock.go: internal/storage/log_stream_reporter_client.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/replicator_mock.go: internal/storage/replicator.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

internal/storage/replicator_client_mock.go: internal/storage/replicator_client.go
	mockgen -self_package github.com/kakao/varlog/internal/storage \
		-package storage \
		-source $< \
		-destination $@

proto/storage_node/mock/replicator_mock.go: $(PROTO) proto/storage_node/replicator.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		-destination $@ \
		github.com/kakao/varlog/proto/storage_node \
		ReplicatorServiceClient,ReplicatorServiceServer,ReplicatorService_ReplicateClient,ReplicatorService_ReplicateServer

proto/storage_node/mock/log_io_mock.go: $(PROTO) proto/storage_node/log_io.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		-destination $@ \
		github.com/kakao/varlog/proto/storage_node \
		LogIOClient,LogIOServer,LogIO_SubscribeClient,LogIO_SubscribeServer

proto/storage_node/mock/log_stream_reporter_mock.go: $(PROTO) proto/storage_node/log_stream_reporter.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		-destination $@ \
		github.com/kakao/varlog/proto/storage_node \
		LogStreamReporterServiceClient,LogStreamReporterServiceServer

proto/storage_node/mock/management_mock.go: $(PROTO) proto/storage_node/management.pb.go
	mockgen -build_flags -mod=vendor \
		-package mock \
		-destination $@ \
		github.com/kakao/varlog/proto/storage_node \
		ManagementClient,ManagementServer

.PHONY: test test_report coverage_report

test:
	tmpfile=$$(mktemp); \
	(TERM=sh $(GO) test $(GOFLAGS) $(GCFLAGS) $(TEST_FLAGS) ./... 2>&1; echo $$? > $$tmpfile) | \
	tee $(BUILD_DIR)/reports/test_output.txt; \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret

test_report:
	cat $(BUILD_DIR)/reports/test_output.txt | \
		go-junit-report > $(BUILD_DIR)/reports/report.xml

coverage_report:
	gocov convert $(BUILD_DIR)/reports/coverage.out | gocov-xml > $(BUILD_DIR)/reports/coverage.xml

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
