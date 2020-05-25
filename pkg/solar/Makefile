LIBSOLAR_SRC := $(shell ls *.go)
LIBSOLAR_TARGET := $(BUILD_DIR)/libsolar.a

$(LIBSOLAR_TARGET): $(LIBSOLAR_SRC)
	$(GO) build -buildmode=archive -o $(LIBSOLAR_TARGET) $(LIBSOLAR_SRC)

clean:
	$(RM) $(LIBSOLAR_TARGET)

.PHONY: clean
