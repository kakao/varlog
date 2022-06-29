package local

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/kakao/varlog/tests/ee/env"
)

type localEnv struct {
	config

	adminServer *Process
}

var _ env.Env = (*localEnv)(nil)

func NewEnv(t *testing.T) env.Env {
	t.Helper()
	return &localEnv{}
}

func (e *localEnv) StartAdminServer(_ context.Context, t *testing.T) {
	t.Helper()
	name := filepath.Join(binDir(), "varlogadm")
	args := []string{
		"start",
		"--cluster-id", e.cid.String(),
		"--listen", "127.0.0.1:9093",
		"--replication-factor", strconv.Itoa(e.replicationFactor),
		"--metadata-repository", "127.0.0.1:10000",
		"--logtostderr",
	}
	e.adminServer = NewProcess(t, name, args, nil)
	e.adminServer.Start(t)
	for i := 0; i < 10; i++ {
		if line, ok := e.adminServer.NextStderrLine(); ok {
			t.Log(line)
		}
	}
}

func (e *localEnv) StopAdminServer(context.Context, *testing.T) {
	e.adminServer.Stop()
	e.adminServer.Wait()
}

func (e *localEnv) String() string {
	return "local"
}
