package logstream

import (
	"testing"
)

func TestReplicateTaskPools(t *testing.T) {
	const repeatCount = 1000

	for range repeatCount {
		rt2 := newReplicateTask()
		rt2.release()
	}
}
