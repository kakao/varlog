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

DOCKER_REPOS := ***REMOVED***


.DEFAULT_GOAL := all
.PHONY: all
all: generate precommit build


# precommit
.PHONY: precommit precommit_lint
precommit: fmt tidy vet test
precommit_lint: fmt tidy vet lint test


# build
BIN_DIR := $(CURDIR)/bin
VMS := $(BIN_DIR)/vms
VARLOGCTL := $(BIN_DIR)/varlogctl
VSN := $(BIN_DIR)/vsn
VMR := $(BIN_DIR)/vmr
SNTOOL := $(BIN_DIR)/sntool
MRTOOL := $(BIN_DIR)/mrtool
RPC_TEST_SERVER := $(BIN_DIR)/rpc_test_server
BENCHMARK := $(BIN_DIR)/benchmark
RPCBENCH_SERVER := $(BIN_DIR)/rpcbench_server 
RPCBENCH_CLIENT := $(BIN_DIR)/rpcbench_client

.PHONY: build vms varlogctl vsn vmr sntool mrtool rpc_test_server benchmark rpcbench
build: vms varlogctl vsn vmr sntool mrtool rpc_test_server benchmark rpcbench
vms:
	$(GO) build $(GCFLAGS) -o $(VMS) cmd/vms/main.go
varlogctl:
	$(GO) build $(GCFLAGS) -o $(VARLOGCTL) $(CURDIR)/cmd/varlogctl
vsn:
	$(GO) build $(GCFLAGS) -o $(VSN) cmd/storagenode/main.go
vmr:
	$(GO) build $(GCFLAGS) -o $(VMR) cmd/metadata_repository/main.go
sntool:
	$(GO) build $(GCFLAGS) -o $(SNTOOL) cmd/sntool/sntool.go
mrtool:
	$(GO) build $(GCFLAGS) -o $(MRTOOL) $(CURDIR)/cmd/mrtool
rpc_test_server:
	$(GO) build -tags rpc_e2e $(GCFLAGS) -o $(RPC_TEST_SERVER) cmd/rpc_test_server/main.go
benchmark:
	$(GO) build $(GCFLAGS) -o $(BENCHMARK) cmd/benchmark/main.go
rpcbench:
	$(GO) build $(GCFLAGS) -o $(RPCBENCH_SERVER) cmd/rpcbench/server/main.go
	$(GO) build $(GCFLAGS) -o $(RPCBENCH_CLIENT) cmd/rpcbench/client/main.go


# testing
REPORTS_DIR := $(CURDIR)/reports
TEST_OUTPUT := $(REPORTS_DIR)/test.out
TEST_REPORT := $(REPORTS_DIR)/test.xml
COVERAGE_OUTPUT_TMP := $(REPORTS_DIR)/coverage.out.tmp
COVERAGE_OUTPUT := $(REPORTS_DIR)/coverage.out
COVERAGE_REPORT := $(REPORTS_DIR)/coverage.xml
BENCH_OUTPUT := $(REPORTS_DIR)/bench.out
BENCH_REPORT := $(REPORTS_DIR)/bench.xml

TEST_FLAGS := -v -race -failfast -count=1

.PHONY: test test_ci test_report coverage_report
test:
	tmpfile=$$(mktemp); \
	(TERM=xterm $(GO) test $(TEST_FLAGS) ./... 2>&1; echo $$? > $$tmpfile) | \
	tee $(TEST_OUTPUT); \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret

test_ci:
	tmpfile=$$(mktemp); \
	(TERM=xterm $(GO) test $(TEST_FLAGS) -coverprofile=$(COVERAGE_OUTPUT_TMP) ./... 2>&1; echo $$? > $$tmpfile) | \
	tee $(TEST_OUTPUT); \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret

test_report:
	cat $(TEST_OUTPUT) | go-junit-report > $(TEST_REPORT)

coverage_report:
	cat $(COVERAGE_OUTPUT_TMP) | grep -v ".pb.go" | grep -v "_mock.go" > $(COVERAGE_OUTPUT)
	gocov convert $(COVERAGE_OUTPUT) | gocov-xml > $(COVERAGE_REPORT)

bench: build
	tmpfile=$$(mktemp); \
	(TERM=xterm $(GO) test -v -run=^$$ -count 1 -bench=. -benchmem ./... 2>&1; echo $$? > $$tmpfile) | \
	tee $(BENCH_OUTPUT); \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret

bench_report:
	cat $(BENCH_OUTPUT) | go-junit-report > $(BENCH_REPORT)

test_e2e:
	tmpfile=$$(mktemp); \
	(TERM=xterm $(GO) test $(TEST_FLAGS) ./tests/e2e -tags=e2e 2>&1; echo $$? > $$tmpfile) | \
	tee $(TEST_OUTPUT); \
	ret=$$(cat $$tmpfile); \
	rm -f $$tmpfile; \
	exit $$ret


# testing on k8s
TEST_DOCKER_CPUS := 8
TEST_DOCKER_MEMORY := 4GB
TEST_POD_NAME := test-e2e
.PHONY: test_docker test_e2e_docker test_e2e_docker_long

test_docker:
	docker run --rm -it \
		--namespace default \
		--cpus $(TEST_DOCKER_CPUS) \
		--memory $(TEST_DOCKER_MEMORY) \
		$(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_REPOS):$(DOCKER_TAG) \
		sh -c "cd /varlog/build && make test"

test_e2e_docker:
	kubectl run --rm -it $(TEST_POD_NAME) \
		--namespace default \
		--image=$(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_REPOS):$(DOCKER_TAG) \
		--image-pull-policy=IfNotPresent \
		--restart=Never \
		--env="VAULT_ADDR=$(VAULT_ADDR)" \
		--env="VAULT_TOKEN=$(VAULT_TOKEN)" \
		--env="VAULT_SECRET_PATH=$(VAULT_SECRET_PATH)" \
		--command -- sh -c "cd /varlog/build && $(GO) test ./tests/e2e -tags=e2e -v -timeout 30m -failfast -count 1 -race -p 1"

test_e2e_docker_long:
	kubectl run --rm -it $(TEST_POD_NAME) \
		--namespace default \
		--image=$(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_REPOS):$(DOCKER_TAG) \
		--image-pull-policy=IfNotPresent \
		--restart=Never \
		--env="VAULT_ADDR=$(VAULT_ADDR)" \
		--env="VAULT_TOKEN=$(VAULT_TOKEN)" \
		--env="VAULT_SECRET_PATH=$(VAULT_SECRET_PATH)" \
		--command -- sh -c "cd /varlog/build && $(GO) test ./tests/e2e -tags=long_e2e -v -timeout 30m -failfast -count 1 -p 1"


# docker
BUILD_DIR := $(CURDIR)/build
DOCKERFILE := $(BUILD_DIR)/release/all-in-one/Dockerfile
IMAGE_REGISTRY := ***REMOVED***
IMAGE_NAMESPACE := varlog
IMAGE_REPOS := all-in-one

VERSION := $(shell cat $(CURDIR)/VERSION)
GIT_HASH := $(shell git describe --always --broken)
BUILD_DATE := $(shell date -u '+%FT%T%z')
DOCKER_TAG := v$(VERSION)-$(GIT_HASH)
# DOCKER_TAG := $(shell $(BUILD_DIR)/d2hub-image-tag.sh)

.PHONY: docker kustomize
docker: 
	docker build \
		--target varlog-all-in-one \
		-f $(DOCKERFILE) \
		-t $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_REPOS):$(DOCKER_TAG) . && \
	docker push $(IMAGE_REGISTRY)/$(IMAGE_NAMESPACE)/$(IMAGE_REPOS):$(DOCKER_TAG)

KUSTOMIZE_ENV := dev
kustomize:
	@sed "s/IMAGE_TAG/$(DOCKER_TAG)/" $(CURDIR)/deploy/k8s/$(KUSTOMIZE_ENV)/kustomization.template.yaml > \
		$(CURDIR)/deploy/k8s/$(KUSTOMIZE_ENV)/kustomization.yaml
	@echo "Run this command to apply: kubectl apply -k $(CURDIR)/deploy/k8s/$(KUSTOMIZE_ENV)/"


# proto
DOCKER_PROTOBUF = $(DOCKER_REPOS)/varlog/protobuf:0.0.3
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
.PHONY: tools fmt lint vet
tools:
	$(GO) install golang.org/x/tools/cmd/goimports
	$(GO) install golang.org/x/lint/golint
	$(GO) install github.com/golang/mock/mockgen
	$(GO) get golang.org/x/tools/cmd/stringer

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
	$(RM) $(TEST_OUTPUT) $(TEST_REPORT)
	$(RM) $(COVERAGE_OUTPUT_TMP) $(COVERAGE_OUTPUT) $(COVERAGE_REPORT)
	$(RM) $(BENCH_OUTPUT) $(BENCH_REPORT)
	$(RM) $(VMS) $(VMC) $(VSN) $(VMR) $(SNTOOL) $(MRTOOL) $(RPC_TEST_SERVER) $(BENCHMARK) $(RPCBENCH_SERVER) $(RPCBENCH_CLIENT)

clean_mock:
	@$(foreach path,$(shell $(GO) list ./... | grep -v vendor | sed -e s#github.com/kakao/varlog/##),$(RM) -f $(path)/*_mock.go;)
