MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

GO := go
GCFLAGS := -gcflags=all='-N -l'
LDFLAGS := -X 'github.com/kakao/varlog/internal/buildinfo.version=$(shell git describe --abbrev=0 --tags)'
LDFLAGS := -ldflags "$(LDFLAGS)"
GOPATH := $(shell $(GO) env GOPATH)
PKGS := $(shell $(GO) list ./... | \
	egrep -v "github.com/kakao/varlog/vendor" | \
	egrep -v "github.com/kakao/varlog/tools" | \
	sed -e "s;github.com/kakao/varlog/;;")

ifneq ($(shell echo $$OSTYPE | egrep "darwin"),)
	export CGO_CFLAGS=-Wno-undef-prefix
	export MallocNanoZone=0
endif

PYTHON := python3

.DEFAULT_GOAL := all
.PHONY: all
all: precommit


# precommit
.PHONY: precommit
precommit: mod-tidy mod-vendor proto generate fmt vet lint build test test_py


# build
BIN_DIR := $(CURDIR)/bin
VARLOGMR := $(BIN_DIR)/varlogmr
VARLOGADM := $(BIN_DIR)/varlogadm
VARLOGSN := $(BIN_DIR)/varlogsn
VARLOGCTL := $(BIN_DIR)/varlogctl
VARLOGCLI := $(BIN_DIR)/varlogcli
MRTOOL := $(BIN_DIR)/mrtool
BENCHMARK := $(BIN_DIR)/benchmark

.PHONY: build varlogmr varlogadm varlogsn varlogctl varlogcli mrtool benchmark
build: varlogmr varlogadm varlogsn varlogctl varlogcli mrtool benchmark
varlogmr:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(VARLOGMR) $(CURDIR)/cmd/varlogmr
varlogadm:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(VARLOGADM) $(CURDIR)/cmd/varlogadm
varlogsn:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(VARLOGSN) $(CURDIR)/cmd/varlogsn
varlogctl:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(VARLOGCTL) $(CURDIR)/cmd/varlogctl
varlogcli:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(VARLOGCLI) $(CURDIR)/cmd/varlogcli
mrtool:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(MRTOOL) $(CURDIR)/cmd/mrtool
benchmark:
	$(GO) build $(GCFLAGS) $(LDFLAGS) -o $(BENCHMARK) $(CURDIR)/cmd/benchmark


# testing
TEST_FLAGS := -race -failfast -count=1 -timeout=20m -p=1
GO_COVERAGE_OUTPUT := coverage.out
GO_COVERAGE_OUTPUT_TMP := $(GO_COVERAGE_OUTPUT).tmp
PYTEST := pytest
PYTHON_COVERAGE_OUTPUT := coverage.xml
.PHONY: test test_coverage test_ci test_report coverage_report test_py
test:
	$(GO) test $(TEST_FLAGS) ./...
	$(PYTEST)

test_coverage:
	$(GO) test $(TEST_FLAGS) -coverprofile=$(GO_COVERAGE_OUTPUT_TMP) -covermode=atomic ./...
	cat $(GO_COVERAGE_OUTPUT_TMP) | grep -v "_mock.go" | grep -v ".pb.go" > $(GO_COVERAGE_OUTPUT)
	$(RM) $(GO_COVERAGE_OUTPUT_TMP)
	$(PYTEST) --cov=./ --cov-report=xml:$(PYTHON_COVERAGE_OUTPUT)

# If you want to specify a directory for grpc_health_probe, enter following:
#   make test_e2e_local TEST_ARGS="-args -grpc-health-probe-executable=<path>"
#
# If you have installed the grpc_health_probe and can look it up by PATH, it is
# unnecessary to enter TEST_ARGS.
TEST_ARGS :=
test_e2e_local: build
	$(GO) test $(TEST_FLAGS) ./tests/ee/... -tags=e2e $(TEST_ARGS)

test_e2e_k8s: build
	$(GO) test $(TEST_FLAGS) ./tests/ee/... -tags=e2e,k8s


# proto
DOCKER_PROTOBUF = ghcr.io/kakao/varlog-protobuf:0.1.0
PROTOC := docker run --rm -u $(shell id -u) -v$(PWD):$(PWD) -w$(PWD) $(DOCKER_PROTOBUF) --proto_path=$(PWD)
PROTO_SRCS := $(shell find . -name "*.proto" -not -path "./vendor/*")
PROTO_PBS := $(PROTO_SRCS:.proto=.pb.go)
PROTO_INCS := -I$(GOPATH)/src -I$(CURDIR)/proto -I$(CURDIR)/vendor

.PHONY: proto proto-check
proto: $(PROTO_PBS)
$(PROTO_PBS): $(PROTO_SRCS)
	@echo $(PROTOC)
	for src in $^ ; do \
		$(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. $$src ; \
	done

proto-check:
	$(MAKE) proto
	$(MAKE) fmt
	git diff --exit-code $(PROTO_PBS)


# go:generate
.PHONY: generate generate-check
generate:
	$(GO) generate ./...

generate-check:
	$(MAKE) generate
	$(MAKE) fmt
	git diff --exit-code


# tools: lint, fmt, vet
.PHONY: tools fmt lint vet mod-tidy mod-tidy-check mod-vendor mod-vendor-check
tools:
	$(GO) install golang.org/x/tools/cmd/goimports@latest
	$(GO) install golang.org/x/lint/golint@latest
	$(GO) install go.uber.org/mock/mockgen@v0.3.0

fmt:
	@echo goimports
	@$(foreach path,$(PKGS),goimports -w -local $(shell $(GO) list -m) ./$(path);)
	@echo gofmt
	@$(foreach path,$(PKGS),gofmt -w -s ./$(path);)

lint:
	docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:v1.54-alpine golangci-lint run

vet:
	@echo govet
	@$(foreach path,$(PKGS),$(GO) vet ./$(path);)

mod-tidy:
	$(GO) mod tidy

mod-tidy-check:
	$(MAKE) mod-tidy
	git diff --exit-code go.mod

mod-vendor:
	$(GO) mod vendor

mod-vendor-check:
	$(MAKE) mod-vendor
	git diff --exit-code vendor


# cleanup
.PHONY: clean clean_mock
clean:
	$(GO) clean
	$(RM) $(GO_COVERAGE_OUTPUT) $(PYTHON_COVERAGE_OUTPUT)
	$(RM) $(VARLOGMR) $(VARLOGADM) $(VARLOGSN) $(VARLOGCTL) $(VARLOGCLI) $(MRTOOL) $(BENCHMARK)

clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),$(RM) -f $(path)/*_mock.go;)
