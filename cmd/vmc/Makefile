VMC_SRCS := $(shell find . -name '*.go')
VMC_MAIN := main.go
VMC_TARGET := $(BUILD_DIR)/vmc

$(VMC_TARGET): $(VMC_SRCS) $(PKG_SRCS) $(INTERNAL_SRCS)
	$(GO) build $(GOFLAGS) $(GCFLAGS) -o $(VMC_TARGET) $(VMC_MAIN)

clean:
	$(RM) $(VMC_TARGET)

.PHONY: clean
