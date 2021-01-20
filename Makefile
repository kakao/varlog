MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build
BIN_DIR := $(MAKEFILE_DIR)/bin

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
all: generate fmt build

VMS := $(BIN_DIR)/vms
VMC := $(BIN_DIR)/vmc
VSN := $(BIN_DIR)/vsn
VMR := $(BIN_DIR)/vmr
SNTOOL := $(BIN_DIR)/sntool

BUILD_OUTPUT := $(VMS) $(VMC) $(VSN) $(VMR) $(SNTOOL)

.PHONY: build vms vmc vsn vmr sntool
build: vms vmc vsn vmr sntool

vms: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMS) cmd/vms/main.go

vmc: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMC) cmd/vmc/main.go

vsn: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VSN) cmd/storagenode/main.go

vmr: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMR) cmd/metadata_repository/main.go

sntool: proto
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(SNTOOL) cmd/sntool/sntool.go

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

TEST_E2E := 0
ifeq ($(TEST_E2E),1)
	TEST_FLAGS := $(TEST_FLAGS) -tags=e2e
endif

.PHONY: test test_report coverage_report
test: build
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
	$(GO) generate ./...

.PHONY: fmt
fmt:
	scripts/fmt.sh

.PHONY: lint
lint:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),golint $(path);)

.PHONY: vet
vet:
	@$(GO) vet ./...

.PHONY: clean
clean:
	$(GO) clean
	$(RM) $(BUILD_OUTPUT)

.PHONY: clean_mock
clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),$(RM) -f $(path)/*_mock.go;)

.PHONY: deps
deps:
	GO111MODULE=off $(GO) get golang.org/x/tools/cmd/goimports
	GO111MODULE=off $(GO) get golang.org/x/lint/golint
	$(GO) get github.com/gogo/protobuf/protoc-gen-gogo@$(GOGO_PROTO_VERSION)
	$(GO) get github.com/golang/mock/mockgen@$(MOCKGEN_VERSION)

.PHONY: check
check: check_proto

.PHONY: check_proto
check_proto:
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

.PHONY: docker image image_vms image_mr image_sn push push_vms push_mr push_sn

VERSION := $(shell cat $(MAKEFILE_DIR)/VERSION)
GIT_HASH := $(shell git describe --always --broken)
BUILD_DATE := $(shell date -u '+%FT%T%z')
DOCKER_TAG := v$(VERSION)-$(GIT_HASH)
# IMAGE_BUILD_DATE := $(shell date -u '+%Y%m%d%H%M')
# DOCKER_TAG := v$(VERSION)-$(GIT_HASH)-$(IMAGE_BUILD_DATE)

docker: image push

image: image_vms image_mr image_sn

image_vms:
	docker build --target varlog-vms -f $(MAKEFILE_DIR)/docker/alpine/Dockerfile -t ***REMOVED***/varlog/varlog-vms:$(DOCKER_TAG) .

image_mr:
	docker build --target varlog-mr -f $(MAKEFILE_DIR)/docker/alpine/Dockerfile -t ***REMOVED***/varlog/varlog-mr:$(DOCKER_TAG) .

image_sn:
	docker build --target varlog-sn -f $(MAKEFILE_DIR)/docker/alpine/Dockerfile -t ***REMOVED***/varlog/varlog-sn:$(DOCKER_TAG) .

push: push_vms push_mr push_sn

push_vms:
	docker push ***REMOVED***/varlog/varlog-vms:$(DOCKER_TAG)

push_mr:
	docker push ***REMOVED***/varlog/varlog-mr:$(DOCKER_TAG)

push_sn:
	docker push ***REMOVED***/varlog/varlog-sn:$(DOCKER_TAG)
