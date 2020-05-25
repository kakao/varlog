SEQUENCER_SRCS := $(shell find . -name '*.go')
SEQUENCER_MAIN := main.go
SEQUENCER_TARGET := $(BUILD_DIR)/sequencer

$(SEQUENCER_TARGET): $(SEQUENCER_SRCS)
	$(GO) build -o $(SEQUENCER_TARGET) $(SEQUENCER__MAIN)

clean:
	$(RM) $(SEQUENCER_TARGET)

.PHONY: clean
