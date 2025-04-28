MAKEFLAGS += --warn-undefined-variables
SHELL := /bin/bash

GO := go
GCFLAGS := -gcflags=all='-N -l'
LDFLAGS := -X 'github.com/kakao/varlog/internal/buildinfo.version=$(shell git describe --abbrev=0 --tags)'
LDFLAGS := -ldflags "$(LDFLAGS)"
COVERFLAGS :=
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
precommit: mod-tidy mod-vendor proto generate fmt vet lint build test


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
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(VARLOGMR) $(CURDIR)/cmd/varlogmr
varlogadm:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(VARLOGADM) $(CURDIR)/cmd/varlogadm
varlogsn:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(VARLOGSN) $(CURDIR)/cmd/varlogsn
varlogctl:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(VARLOGCTL) $(CURDIR)/cmd/varlogctl
varlogcli:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(VARLOGCLI) $(CURDIR)/cmd/varlogcli
mrtool:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(MRTOOL) $(CURDIR)/cmd/mrtool
benchmark:
	$(GO) build $(GCFLAGS) $(LDFLAGS) $(COVERFLAGS) -o $(BENCHMARK) $(CURDIR)/cmd/benchmark


# testing
TEST_FLAGS := -race -failfast -count=1 -timeout=20m
COVDATA_DIR := $(CURDIR)/covdata
GO_COVERAGE_OUTPUT := coverage.out
GO_COVERAGE_OUTPUT_TMP := $(GO_COVERAGE_OUTPUT).tmp
PYTEST := pytest
PYTHON_COVERAGE_OUTPUT := coverage.xml
.PHONY: test test_coverage generate_coverage_profile test_e2e_local_coverage test_e2e_local test_e2e_k8s
test:
	$(GO) test $(TEST_FLAGS) ./...
	$(PYTEST)

test_coverage:
	rm -rf $(COVDATA_DIR)/unit && mkdir $(COVDATA_DIR)/unit
	$(GO) test $(TEST_FLAGS) -cover -coverpkg ./... -covermode=atomic ./... -args -test.gocoverdir=$(COVDATA_DIR)/unit
	$(PYTEST) --cov=./ --cov-report=xml:$(PYTHON_COVERAGE_OUTPUT)

generate_coverage_profile:
	rm -rf $(COVDATA_DIR)/merged && mkdir $(COVDATA_DIR)/merged
	$(GO) tool covdata merge -i=$(COVDATA_DIR)/unit,$(COVDATA_DIR)/ee -o $(COVDATA_DIR)/merged
	$(GO) tool covdata textfmt -i=$(COVDATA_DIR)/merged -o $(GO_COVERAGE_OUTPUT_TMP)
	cat $(GO_COVERAGE_OUTPUT_TMP) | grep -v "_mock.go" | grep -v ".pb.go" > $(GO_COVERAGE_OUTPUT)
	$(RM) $(GO_COVERAGE_OUTPUT_TMP)

# If you want to specify a directory for grpc_health_probe, enter following:
#   make test_e2e_local TEST_ARGS="-args -grpc-health-probe-executable=<path>"
#
# If you have installed the grpc_health_probe and can look it up by PATH, it is
# unnecessary to enter TEST_ARGS.
TEST_ARGS :=
test_e2e_local_coverage:
	make build COVERFLAGS="-cover -covermode=atomic -coverpkg ./..."
	rm -rf $(COVDATA_DIR)/ee && mkdir $(COVDATA_DIR)/ee
	GOCOVERDIR=$(COVDATA_DIR)/ee $(GO) test $(TEST_FLAGS) ./tests/ee/... -tags=e2e $(TEST_ARGS)

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

.PHONY: proto proto-check proto-patch apply-proto-patch
proto: $(PROTO_PBS)
$(PROTO_PBS): $(PROTO_SRCS)
	for src in $^ ; do \
		$(PROTOC) $(PROTO_INCS) \
		--gogo_out=plugins=grpc,Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,paths=source_relative:. $$src ; \
	done
	$(MAKE) fmt
	$(MAKE) apply-proto-patch

PATCH_DIR := $(CURDIR)/proto/patches
PATCHED_PBS := $(CURDIR)/proto/snpb/replicator.pb.go
proto-patch:
	@for pb_file in $(PATCHED_PBS); do \
		echo "Generating patch for $$pb_file"; \
		base_name=$$(basename "$$pb_file"); \
		relative_pb_path="$${pb_file#$(CURDIR)/proto/}"; \
		relative_pb_dir=$$(dirname "$$relative_pb_path"); \
		target_patch_dir="$(PATCH_DIR)/$$relative_pb_dir"; \
		patch_file="$$target_patch_dir/$$base_name.patch"; \
		diff_output=$$(git diff -- "$$pb_file"); \
		if [ -n "$$diff_output" ]; then \
			echo "$$diff_output" > "$$patch_file"; \
			echo "Patch file created: $$patch_file"; \
		fi; \
	done

apply-proto-patch:
	@find $(PATCH_DIR) -type f -name "*.patch" -print0 | while IFS= read -r -d $$'\0' patch_file; do \
		echo "--- Processing: $$patch_file"; \
		if git apply -R --check "$$patch_file" > /dev/null 2>&1; then \
			echo "    Already applied (skip)"; \
			continue; \
		fi; \
		if ! git apply --check "$$patch_file"; then \
			echo "    Potential conflict" >&2; \
			exit 1; \
		fi; \
		if ! git apply --verbose "$$patch_file"; then \
			echo "    Failed unexpectedly" >&2; \
			exit 1; \
		fi; \
		echo "    Patch applied"; \
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
.PHONY: fmt lint vet mod-tidy mod-tidy-check mod-vendor mod-vendor-check

fmt:
	@echo goimports
	@$(foreach path,$(PKGS),go tool goimports -w -local $(shell $(GO) list -m) ./$(path);)
	@echo gofmt
	@$(foreach path,$(PKGS),gofmt -w -s ./$(path);)

lint:
	docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:v2.1.6-alpine golangci-lint run

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
	$(RM) -rf $(GO_COVERAGE_OUTPUT) $(PYTHON_COVERAGE_OUTPUT) $(COVDATA_DIR)/*
	$(RM) $(VARLOGMR) $(VARLOGADM) $(VARLOGSN) $(VARLOGCTL) $(VARLOGCLI) $(MRTOOL) $(BENCHMARK)

clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),$(RM) -f $(path)/*_mock.go;)
