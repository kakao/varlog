export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO := go
LDFLAGS :=
CFLAGS := -gcflags "-N -l"
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I ${MAKEFILE_DIR}/proto -I ${MAKEFILE_DIR}/vendor -I .

all : proto libsolar sequencer storage_node sequencer_client

SOLAR_PROTO := proto/solar
SEQUENCER_PROTO := proto/sequencer
STORAGE_NODE_PROTO := proto/storage_node
METADATA_REPOSITORY_PROTO := proto/metadata_repository
PROTO := $(SOLAR_PROTO) $(SEQUENCER_PROTO) $(STORAGE_NODE_PROTO) $(METADATA_REPOSITORY_PROTO)
proto : $(PROTO)

SEQUENCER := cmd/sequencer
sequencer : $(SEQUENCER_PROTO) $(SEQUENCER)

STORAGE_NODE := cmd/storage_node
storage_node : $(STORAGE_NODE_PROTO) $(STORAGE_NODE)

LIBSOLAR := pkg/solar
libsolar : $(SEQUENCER_PROTO) $(LIBSOLAR)

SEQUENCER_CLIENT := cmd/sequencer_client
sequencer_client : $(SEQUENCER_PROTO) $(SEQUENCER_CLIENT)

SUBDIRS := $(PROTO) $(SEQUENCER) $(STORAGE_NODE) $(LIBSOLAR) $(SEQUENCER_CLIENT)
subdirs : $(SUBDIRS)

mockgen : pkg/libsolar/mock/sequencer_mock.go pkg/libsolar/mock/storage_node_mock.go

pkg/libsolar/mock/sequencer_mock.go : $(PROTO) proto/sequencer/sequencer.pb.go
	mockgen -source=proto/sequencer/sequencer.pb.go -package mock SequencerServiceClient > pkg/libsolar/mock/sequencer_mock.go

pkg/libsolar/mock/storage_node_mock.go : $(PROTO) proto/storage_node/storage_node.pb.go
	mockgen -source=proto/storage_node/storage_node.pb.go -package mock StorageNodeServiceClient > pkg/libsolar/mock/storage_node_mock.go

$(SUBDIRS) :
	$(MAKE) -C $@

clean :
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done
	$(RM) proto/sequencer/sequencer.pb.go
	$(RM) proto/storage_node/storage_node.pb.go

.PHONY : all clean subdirs $(SUBDIRS) mockgen
