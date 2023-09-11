package varlogtest

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

type config struct {
	clusterID         types.ClusterID
	replicationFactor int
	initialMRNodes    []varlogpb.MetadataRepositoryNode
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		initialMRNodes: []varlogpb.MetadataRepositoryNode{
			{
				NodeID:  types.NewNodeIDFromURL("http://127.0.10.1:9091"),
				RaftURL: "http://127.0.10.1:9091",
				RPCAddr: "127.0.10.1:9092",
			},
			{
				NodeID:  types.NewNodeIDFromURL("http://127.0.10.2:9091"),
				RaftURL: "http://127.0.10.2:9091",
				RPCAddr: "127.0.10.2:9092",
			},
			{
				NodeID:  types.NewNodeIDFromURL("http://127.0.10.3:9091"),
				RaftURL: "http://127.0.10.3:9091",
				RPCAddr: "127.0.10.3:9092",
			},
		},
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.clusterID.Invalid() {
		return fmt.Errorf("invalid cluster id %d", cfg.clusterID)
	}
	if cfg.replicationFactor < 1 {
		return fmt.Errorf("invalid replication factor %d", cfg.replicationFactor)
	}
	for _, mrn := range cfg.initialMRNodes {
		if mrn.NodeID == types.InvalidNodeID {
			return fmt.Errorf("invalid metadata repository node id %d", mrn.NodeID)
		}
	}
	return nil
}

type Option interface {
	apply(*config)
}

type funcOption struct {
	f func(*config)
}

func newFuncOption(f func(*config)) *funcOption {
	return &funcOption{f: f}
}

func (fo *funcOption) apply(cfg *config) {
	fo.f(cfg)
}

func WithClusterID(cid types.ClusterID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.clusterID = cid
	})
}

func WithReplicationFactor(repfactor int) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicationFactor = repfactor
	})
}

func WithInitialMetadataRepositoryNodes(mrns ...varlogpb.MetadataRepositoryNode) Option {
	return newFuncOption(func(cfg *config) {
		cfg.initialMRNodes = mrns
	})
}
