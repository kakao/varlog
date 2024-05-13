package storagenode

import (
	"errors"
	"fmt"
	"path/filepath"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/volume"
	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultBallastSize               = "0B"
	DefaultMaxLogStreamReplicasCount = -1

	DefaultAppendPipelineSize = 8
	MinAppendPipelineSize     = 1
	MaxAppendPipelineSize     = 16
)

type config struct {
	cid                             types.ClusterID
	snid                            types.StorageNodeID
	listen                          string
	advertise                       string
	ballastSize                     int64
	defaultGRPCServerOptions        []grpc.ServerOption
	defaultGRPCDialOptions          []grpc.DialOption
	maxLogStreamReplicasCount       int32
	appendPipelineSize              int32
	volumes                         []string
	defaultLogStreamExecutorOptions []logstream.ExecutorOption
	pprofOpts                       []pprof.Option
	defaultStorageOptions           []storage.Option
	logger                          *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		maxLogStreamReplicasCount: DefaultMaxLogStreamReplicasCount,
		appendPipelineSize:        DefaultAppendPipelineSize,
		logger:                    zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func (cfg *config) validate() error {
	if cfg.snid.Invalid() {
		return fmt.Errorf("storage node: invalid id %d", int32(cfg.snid))
	}
	if len(cfg.listen) == 0 {
		return errors.New("storage node: no listen address")
	}
	if cfg.logger == nil {
		return errors.New("storage node: no logger")
	}
	if err := cfg.validateVolumes(); err != nil {
		return fmt.Errorf("storage node: invalid volume: %w", err)
	}
	if cfg.appendPipelineSize < MinAppendPipelineSize || cfg.appendPipelineSize > MaxAppendPipelineSize {
		return fmt.Errorf("storage node: invalid append pipeline size \"%d\"", cfg.appendPipelineSize)
	}
	return nil
}

func (cfg *config) validateVolumes() error {
	volumes := make([]string, 0, len(cfg.volumes))
	visited := make(map[string]bool, len(cfg.volumes))
	for _, vol := range cfg.volumes {
		norm, err := filepath.Abs(vol)
		if err != nil {
			return fmt.Errorf("storage node: invalid volume: %w", err)
		}

		if err := volume.WritableDirectory(norm); err != nil {
			return fmt.Errorf("storage node: invalid volume: %w", err)
		}

		if visited[norm] {
			return fmt.Errorf("storage node: duplicated volume %s", vol)
		}

		visited[norm] = true
		volumes = append(volumes, norm)
	}
	cfg.volumes = volumes
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
		cfg.cid = cid
	})
}

func WithStorageNodeID(snid types.StorageNodeID) Option {
	return newFuncOption(func(cfg *config) {
		cfg.snid = snid
	})
}

func WithListenAddress(listen string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.listen = listen
	})
}

func WithAdvertiseAddress(advertise string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.advertise = advertise
	})
}

func WithBallastSize(ballastSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.ballastSize = ballastSize
	})
}

func WithDefaultGRPCServerOptions(grpcServerOptions ...grpc.ServerOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultGRPCServerOptions = grpcServerOptions
	})
}

func WithDefaultGRPCDialOptions(grpcDialOptions ...grpc.DialOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultGRPCDialOptions = grpcDialOptions
	})
}

func WithDefaultLogStreamExecutorOptions(defaultLSEOptions ...logstream.ExecutorOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultLogStreamExecutorOptions = defaultLSEOptions
	})
}

func WithMaxLogStreamReplicasCount(maxLogStreamReplicasCount int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.maxLogStreamReplicasCount = maxLogStreamReplicasCount
	})
}

func WithAppendPipelineSize(appendPipelineSize int32) Option {
	return newFuncOption(func(cfg *config) {
		cfg.appendPipelineSize = appendPipelineSize
	})
}

func WithVolumes(volumes ...string) Option {
	return newFuncOption(func(cfg *config) {
		cfg.volumes = volumes
	})
}

func WithPProfOptions(pprofOpts ...pprof.Option) Option {
	return newFuncOption(func(cfg *config) {
		cfg.pprofOpts = pprofOpts
	})
}

func WithDefaultStorageOptions(defaultStorageOpts ...storage.Option) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultStorageOptions = defaultStorageOpts
	})
}

func WithLogger(logger *zap.Logger) Option {
	return newFuncOption(func(cfg *config) {
		cfg.logger = logger
	})
}
