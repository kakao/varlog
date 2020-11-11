MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GOGO_PROTO_VERSION := v1.3.1
MOCKGEN_VERSION := v1.4.4

GO := go
GOPATH := $(shell $(GO) env GOPATH)
LDFLAGS :=
GOFLAGS := -race
GCFLAGS := -gcflags=all='-N -l'

PROTOC := protoc
GRPC_GO_PLUGIN := protoc-gen-gogo
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .
PROTO_SRCS := $(wildcard proto/*/*.proto)
PROTO_PBS := $(PROTO_SRCS:.proto=.pb.go)
HAS_PROTOC := $(shell which $(PROTOC) > /dev/null && echo true || echo false)
HAS_VALID_PROTOC := false
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC := $(shell $(PROTOC) --version | grep -q "libprotoc 3" > /dev/null && echo true || echo false)
endif
HAS_GRPC_PLUGIN := $(shell which $(GRPC_GO_PLUGIN) > /dev/null && echo true || echo false)

.PHONY: all
all: check proto generate fmt build

VMS := build/vms
VMC := build/vmc
VSN := build/vsn
VMR := build/vmr
BUILD_OUTPUT := $(VMS) $(VMC) $(VSN) $(VMR)

.PHONY: build vms vmc vsn vmr
build: vms vmc vsn vmr

vms: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMS) cmd/vms/main.go

vmc: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMC) cmd/vmc/main.go

vsn: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VSN) cmd/storagenode/main.go

vmr: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMR) cmd/metadata_repository/main.go

.PHONY: proto
proto: $(PROTO_PBS)
$(PROTO_PBS): $(PROTO_SRCS)
	for src in $^ ; do \
		$(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. $$src ; \
	done

TEST_COUNT := 1
TEST_FLAGS := -count $(TEST_COUNT) -p 1

ifneq ($(TEST_CPU),)
	TEST_FLAGS := $(TEST_FLAGS) -cpu $(TEST_CPU)
endif

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

.PHONY: test test_report coverage_report
test: check proto generate fmt 
	tmpfile=$$(mktemp); \
	(TERM=sh $(GO) test $(GOFLAGS) $(GCFLAGS) $(TEST_FLAGS) ./... 2>&1; echo $$? > $$tmpfile) | \
	tee $(BUILD_DIR)/reports/test_output.txt; \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret

test_report:
	cat $(BUILD_DIR)/reports/test_output.txt | \
		go-junit-report > $(BUILD_DIR)/reports/report.xml
	rm $(BUILD_DIR)/reports/test_output.txt

coverage_report:
	gocov convert $(BUILD_DIR)/reports/coverage.out | gocov-xml > $(BUILD_DIR)/reports/coverage.xml

.PHONY: generate
generate:
	go generate ./...

.PHONY: fmt
fmt:
	scripts/fmt.sh

.PHONY: lint
lint:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.daumkakao.com/varlog/varlog/##),golint $(path);)

.PHONY: vet
vet:
	@$(GO) vet ./...

.PHONY: clean
clean:
	$(GO) clean
	$(RM) $(BUILD_OUTPUT)

.PHONY: clean_mock
clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.daumkakao.com/varlog/varlog/##),$(RM) -f $(path)/*_mock.go;)

.PHONY: deps
deps:
	$(GO) get golang.org/x/tools/cmd/goimports
	$(GO) get golang.org/x/lint/golint
	$(GO) get github.com/gogo/protobuf/protoc-gen-gogo@$(GOGO_PROTO_VERSION)
	$(GO) get github.com/gogo/protobuf/gogoproto@$(GOGO_PROTO_VERSION)
	$(GO) get github.com/gogo/protobuf/proto@$(GOGO_PROTO_VERSION)
	$(GO) get github.com/gogo/protobuf/jsonpb@$(GOGO_PROTO_VERSION)
	$(GO) get github.com/golang/mock/mockgen@$(MOCKGEN_VERSION)

.PHONY: check
check:
ifneq ($(HAS_PROTOC),true)
	@echo "error: $(PROTOC) not installed"
	@false
endif
	@echo "ok: $(PROTOC)"
ifneq ($(HAS_VALID_PROTOC),true)
	@echo "error: $(shell $(PROTOC) --version) invalid version"
	@false
endif
	@echo "ok: $(shell $(PROTOC) --version)"
ifneq ($(HAS_GRPC_PLUGIN),true)
	@echo "error: $(GRPC_GO_PLUGIN) not installed"
	@false
endif
	@echo "ok: $(GRPC_GO_PLUGIN)"
