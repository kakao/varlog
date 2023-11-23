package x

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var basepath string

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)
}

func NewMetadataRepository(t *testing.T, networkName string) testcontainers.Container {
	cr := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       filepath.Join(basepath, "../../."),
			Dockerfile:    "build/Dockerfile",
			PrintBuildLog: true,
		},
		Cmd: []string{
			"python3",
			"/varlog/bin/start_varlogmr.py",
			"--cluster-id=1",
			"--raft-address=http://127.0.0.1:10000",
			"--listen=0.0.0.0:9092",
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
			testcontainers.BindMount(filepath.Join(basepath, "../../mr1"), "/varlog/data/mr"),
		),
		Networks: []string{networkName},
	}
	container, err := testcontainers.GenericContainer(context.TODO(), testcontainers.GenericContainerRequest{
		ContainerRequest: cr,
		Started:          true,
	})
	require.NoError(t, err)
	port, err := container.MappedPort(context.TODO(), "9092/tcp")
	require.NoError(t, err)
	t.Logf("exposed port: %d", port.Int())
	require.NoError(t, err)

	ip, err := container.ContainerIP(context.TODO())
	require.NoError(t, err)
	t.Logf("container ip: %s", ip)

	return container

}

func NewAdmin(t *testing.T, networkName string, mrAddr string) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:       filepath.Join(basepath, "../../."),
			Dockerfile:    "build/Dockerfile",
			PrintBuildLog: true,
			BuildArgs:     map[string]*string{},
		},
		Cmd: []string{
			"/varlog/bin/varlogadm",
			"start",
			"--cluster-id=1",
			"--listen=127.0.0.1:9093",
			"--replication-factor=3",
			"--mr-address=" + mrAddr,
			"--logtostderr",
			"--logdir=/varlog/data/adm/logs",
			"--loglevel=DEBUG",
		},
		ExposedPorts: []string{"9093/tcp"},
		// WaitingFor:   wait.ForListeningPort("9093/tcp"),
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(filepath.Join(basepath, "../../adm"), "/varlog/data/adm"),
		),
		Networks: []string{networkName},
	}
	container, err := testcontainers.GenericContainer(context.TODO(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	port, err := container.MappedPort(context.TODO(), "9093/tcp")
	require.NoError(t, err)
	t.Logf("exposed port: %d", port.Int())
	require.NoError(t, err)

	return container
}
