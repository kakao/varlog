package logstream

import "fmt"

var (
	errStorageIsNil  = fmt.Errorf("log stream: storage is nil")
	errExecutorIsNil = fmt.Errorf("log stream: executor is nil")
	errLoggerIsNil   = fmt.Errorf("log stream: logger is nil")
)

func validateQueueCapacity(name string, capacity int) error {
	if capacity < minQueueCapacity {
		return fmt.Errorf("log stream: %s queue capacity must be at least %d", name, minQueueCapacity)
	}
	if capacity > maxQueueCapacity {
		return fmt.Errorf("log stream: %s queue capacity must be less than %d", name, maxQueueCapacity)
	}
	return nil
}
