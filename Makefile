export

MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
BUILD_DIR := $(MAKEFILE_DIR)/build

GO := go
LDFLAGS :=
# CFLAGS := -gcflags "-N -l"
CFLAGS := 
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I vendor -I .

SEQUENCER_PROTO := proto/sequencer
PROTO := $(SEQUENCER_PROTO)
proto: $(PROTO)

SEQUENCER_SERVER := cmd/sequencer_server
sequencer_server: $(SEQUENCER_PROTO) $(SEQUENCER_SERVER)

SEQUENCER_CLIENT_LIB := pkg/libsequencer
sequencer_client_lib: $(SEQUENCER_PROTO) $(SEQUENCER_CLIENT_LIB)

SEQUENCER_CLIENT := cmd/sequencer_client
sequencer_client: $(SEQUENCER_PROTO) $(SEQUENCER_CLIENT)

SUBDIRS := $(SEQUENCER_PROTO) $(SEQUENCER_SERVER) $(SEQUENCER_CLIENT_LIB) $(SEQUENCER_CLIENT)
subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

.PHONY: clean subdirs $(SUBDIRS)
