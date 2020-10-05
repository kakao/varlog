VMS_SRCS := $(shell find . -name '*.go')
VMS_MAIN := main.go
VMS_TARGET := $(BUILD_DIR)/vms

$(VMS_TARGET): $(VMS_SRCS) $(PKG_SRCS) $(INTERNAL_SRCS)
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMS_TARGET) $(VMS_MAIN)

clean:
	$(RM) $(VMS_TARGET)

.PHONY: clean
