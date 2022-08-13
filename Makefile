MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

GO := go
GCFLAGS := -gcflags=all='-N -l'
GOPATH := $(shell $(GO) env GOPATH)
PKGS := $(shell $(GO) list ./... | \
	egrep -v "github.com/kakao/varlog/vendor" | \
	egrep -v "github.com/kakao/varlog/tools" | \
	sed -e "s;github.com/kakao/varlog/;;")

ifneq ($(shell echo $$OSTYPE | egrep "darwin"),)
	export CGO_CFLAGS=-Wno-undef-prefix
endif

PYTHON := python3

.DEFAULT_GOAL := all
.PHONY: all
all: generate precommit build


# precommit
.PHONY: precommit precommit_lint
precommit: fmt tidy vet test test_py
precommit_lint: fmt tidy vet lint test test_py


# build
BIN_DIR := $(CURDIR)/bin
VMR := $(BIN_DIR)/vmr
VARLOGADM := $(BIN_DIR)/varlogadm
VARLOGSN := $(BIN_DIR)/varlogsn
VARLOGCTL := $(BIN_DIR)/varlogctl
VARLOGCLI := $(BIN_DIR)/varlogcli
MRTOOL := $(BIN_DIR)/mrtool
STRESS := $(BIN_DIR)/stress

.PHONY: build vmr varlogadm varlogsn varlogctl varlogcli mrtool stress
build: vmr varlogadm varlogsn varlogctl varlogcli mrtool stress
vmr:
	$(GO) build $(GCFLAGS) -o $(VMR) $(CURDIR)/cmd/varlogmr
varlogadm:
	$(GO) build $(GCFLAGS) -o $(VARLOGADM) $(CURDIR)/cmd/varlogadm
varlogsn:
	$(GO) build $(GCFLAGS) -o $(VARLOGSN) $(CURDIR)/cmd/varlogsn
varlogctl:
	$(GO) build $(GCFLAGS) -o $(VARLOGCTL) $(CURDIR)/cmd/varlogctl
varlogcli:
	$(GO) build $(GCFLAGS) -o $(VARLOGCLI) $(CURDIR)/cmd/varlogcli
mrtool:
	$(GO) build $(GCFLAGS) -o $(MRTOOL) $(CURDIR)/cmd/mrtool
stress:
	$(GO) build $(GCFLAGS) -o $(STRESS) $(CURDIR)/cmd/stress


# testing
TEST_FLAGS := -v -race -failfast -count=1
GO_COVERAGE_OUTPUT := coverage.out
PYTEST := pytest
PYTHON_COVERAGE_OUTPUT := coverage.xml
.PHONY: test test_coverage test_ci test_report coverage_report test_py
test:
	$(GO) test $(TEST_FLAGS) ./...
	$(PYTEST)

test_coverage:
	$(GO) test $(TEST_FLAGS) -coverprofile=$(GO_COVERAGE_OUTPUT) -covermode=atomic ./...
	$(PYTEST) --cov=./ --cov-report=xml:$(PYTHON_COVERAGE_OUTPUT)

test_e2e:
	$(GO) test $(TEST_FLAGS) ./tests/ee/... -tags=e2e


# proto
DOCKER_PROTOBUF = ghcr.io/kakao/varlog-protobuf:0.1.0
PROTOC := docker run --rm -u $(shell id -u) -v$(PWD):$(PWD) -w$(PWD) $(DOCKER_PROTOBUF) --proto_path=$(PWD)
PROTO_SRCS := $(shell find . -name "*.proto" -not -path "./vendor/*")
PROTO_PBS := $(PROTO_SRCS:.proto=.pb.go)
PROTO_INCS := -I$(GOPATH)/src -I$(CURDIR)/proto -I$(CURDIR)/vendor

.PHONY: proto
proto: $(PROTO_PBS)
$(PROTO_PBS): $(PROTO_SRCS)
	@echo $(PROTOC)
	for src in $^ ; do \
		$(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. $$src ; \
	done


# go:generate
.PHONY: generate
generate:
	$(GO) generate ./...


# tools: lint, fmt, vet
.PHONY: fmt lint vet
fmt:
	@echo goimports
	@$(foreach path,$(PKGS),goimports -w -local $(shell $(GO) list -m) ./$(path);)
	@echo gofmt
	@$(foreach path,$(PKGS),gofmt -w -s ./$(path);)

lint:
	@echo golint
	@$(foreach path,$(PKGS),golint -set_exit_status ./$(path);)

vet:
	@echo govet
	@$(foreach path,$(PKGS),$(GO) vet ./$(path);)

tidy:
	$(GO) mod tidy


# cleanup
.PHONY: clean clean_mock
clean:
	$(GO) clean
	$(RM) $(GO_COVERAGE_OUTPUT) $(PYTHON_COVERAGE_OUTPUT)
	$(RM) $(VMR) $(VARLOGADM) $(VARLOGSN) $(VARLOGCTL) $(VARLOGCLI) $(MRTOOL) $(STRESS)

clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),$(RM) -f $(path)/*_mock.go;)
