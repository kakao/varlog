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

TEST_DIRS := $(sort $(dir $(shell find . -name '*_test.go')))

all : proto libvarlog storage_node metadata_repository

SOLAR_PROTO := proto/varlog
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)

proto : protoc gogoproto $(PROTO)

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

test:
	for dir in $(TEST_DIRS); do \
		$(GO) test $(GOFLAGS) $(GCFLAGS) -v -c $$dir ; \
	done

clean :
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

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
