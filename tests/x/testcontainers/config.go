package containers

import (
	"github.com/kakao/varlog/pkg/types"
)

const (
	defaultHostAddr = "127.0.0.1"
	logsDir         = "logs"

	defaultVarlogMRPort     = "9092"
	defaultVarlogMRRaftPort = "10000"
	raftDataDir             = "raftdata"
)

type Option interface {
	apply(*config)
}

type config struct {
	cid       types.ClusterID
	repfactor int
	datadir   string
}

func WithClusterID() {
}

func WithReplicationFactor() {
}

func WithDataDir() {
}

func WithListenAddress() {
}

func WithRaftAddress() {
}

type MROption interface {
	apply(*config)
}
