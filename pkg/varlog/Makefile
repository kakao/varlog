LIBVARLOG_SRC := $(shell ls *.go)
LIBVARLOG_TARGET := $(BUILD_DIR)/libvarlog.a

$(LIBVARLOG_TARGET): $(LIBVARLOG_SRC)
	PATH=$$PATH:$(GO_HOME)/bin $(GO) build $(GOFLAGS) -buildmode=archive $(GCFLAGS) -o $(LIBVARLOG_TARGET) $(LIBVARLOG_SRC)

clean:
	$(RM) $(LIBVARLOG_TARGET)

.PHONY: clean
