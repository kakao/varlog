package grpc

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	DefaultHealthProbeWaitFor = 10 * time.Second
	DefaultHealthProbeTick    = time.Second
)

func HealthProbe(t *testing.T, executable, addr string, waitForAndTick ...time.Duration) {
	require.LessOrEqual(t, len(waitForAndTick), 2)
	waitFor := DefaultHealthProbeWaitFor
	tick := DefaultHealthProbeTick
	if len(waitForAndTick) > 0 {
		waitFor = waitForAndTick[0]
	}
	if len(waitForAndTick) > 1 {
		tick = waitForAndTick[1]
	}
	require.Eventually(t, func() bool {
		cmd := exec.Command(executable, "-addr", addr)
		return cmd.Run() == nil
	}, waitFor, tick)
}
