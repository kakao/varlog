package varlogmr

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var basepath string

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)
}

type VarlogmrContainer struct {
	config
	testcontainers.Container
}

func RunVarlogmrContainer(ctx context.Context, opts ...Option) (*VarlogmrContainer, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	vc := &VarlogmrContainer{
		config: cfg,
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       filepath.Join(basepath, "../../../../"),
			Dockerfile:    "build/Dockerfile",
			PrintBuildLog: true,
		},
		Cmd: []string{
			"python3",
			"/varlog/bin/start_varlogmr.py",
			"--cluster-id=1",
			"--raft-address=http://127.0.0.1:10000",
			"--listen=127.0.0.1:9092",
			"--seed=127.0.0.1:9092",
			"--admin=127.0.0.1:9093",
			"--replication-factor=3",
			"--reportcommitter-read-buffer-size=128kb",
			"--reportcommitter-write-buffer-size=128kb",
			"--raft-dir=/varlog/data/mr/raftdata",
			"--retry-interval-seconds=10",
		},
		Env: map[string]string{
			"LOGTOSTDERR": "true",
			"LOG_DIR":     "/varlog/data/mr/logs",
		},
		ExposedPorts: []string{"9092/tcp", "10000/tcp"},
		WaitingFor:   wait.ForListeningPort("9092/tcp"),
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(filepath.Join(basepath, "../../../mr1"), "/varlog/data/mr"),
		),
	}

	gcr := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		return nil, err
	}
	vc.Container = c

	return vc, nil
}

func (vc *VarlogmrContainer) RAFTAddress() string {
	return "http://" + vc.host + ":" + strconv.Itoa(vc.raftPort)
}
