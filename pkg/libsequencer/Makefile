SEQUENCER_CLIENT_LIB_SRC := $(shell find . -name '*.go')
SEQUENCER_CLIENT_LIB_OBJ := $(BUILD_DIR)/sequencer.a

$(SEQUENCER_CLIENT_LIB_OBJ): $(SEQUENCER_CLIENT_LIB_SRC)
	$(GO) build -buildmode=archive -o $(SEQUENCER_CLIENT_LIB_OBJ) sequencer.go

clean:
	$(RM) -f $(SEQUENCER_CLIENT_LIB_OBJ)

.PHONY: clean
