export

GO := go
LDFLAGS :=
CFLAGS := -gcflags "-N -l"
PROTOC := protoc
PROTO_INCS := -I ${GOPATH}/src -I vendor -I .

SEQUENCER_PROTO := proto/sequencer
PROTO := $(SEQUENCER_PROTO)
proto: $(PROTO)

SEQUENCER_SERVER := cmd/sequencer_server
sequencer_server: $(SEQUENCER_PROTO) $(SEQUENCER_SERVER)

SUBDIRS := $(SEQUENCER_PROTO) $(SEQUENCER_SERVER)
subdirs: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean; \
	done

.PHONY: clean subdirs $(SUBDIRS)
