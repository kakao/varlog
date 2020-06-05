LIBVARLOG_SRC := $(shell ls *.go)
LIBVARLOG_TARGET := $(BUILD_DIR)/libvarlog.a

$(LIBVARLOG_TARGET): $(LIBVARLOG_SRC)
	$(GO) build -buildmode=archive $(GOFLAGS) $(GCFLAGS) -o $(LIBVARLOG_TARGET) $(LIBVARLOG_SRC)

clean:
	$(RM) $(LIBVARLOG_TARGET)

.PHONY: clean
