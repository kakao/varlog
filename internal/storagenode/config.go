package storagenode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/fputil"
)

const (
	DefaultServerReadBufferSize           = 32 << 10
	DefaultServerWriteBufferSize          = 32 << 10
	DefaultServerMaxRecvSize              = 4 << 20
	DefaultReplicateClientReadBufferSize  = 32 << 10
	DefaultReplicateClientWriteBufferSize = 32 << 10
)

type config struct {
	cid                             types.ClusterID
	snid                            types.StorageNodeID
	listen                          string
	advertise                       string
	ballastSize                     int64
	grpcServerReadBufferSize        int64
	grpcServerWriteBufferSize       int64
	grpcServerMaxRecvMsgSize        int64
	replicateClientReadBufferSize   int64
	replicateClientWriteBufferSize  int64
	volumes                         []string
	defaultLogStreamExecutorOptions []logstream.ExecutorOption
	pprofOpts                       []pprof.Option
	defaultStorageOptions           []storage.Option
	logger                          *zap.Logger
}

func newConfig(opts []Option) (config, error) {
	cfg := config{
		grpcServerReadBufferSize:       DefaultServerReadBufferSize,
		grpcServerWriteBufferSize:      DefaultServerWriteBufferSize,
		grpcServerMaxRecvMsgSize:       DefaultServerMaxRecvSize,
		replicateClientReadBufferSize:  DefaultReplicateClientReadBufferSize,
		replicateClientWriteBufferSize: DefaultReplicateClientWriteBufferSize,
		logger:                         zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.ensureVolumes(); err != nil {
		return cfg, err
	}
	return cfg, cfg.validate()
}

func (cfg config) validate() error {
	if len(cfg.listen) == 0 {
		return errors.New("storage node: no listen address")
	}
	if cfg.logger == nil {
		return errors.New("storage node: no logger")
	}
	if err := cfg.validateVolumes(); err != nil {
		return fmt.Errorf("storage node: invalid volumes: %w", err)
	}
	return nil
}

func (cfg config) validateVolumes() error {
	volumes := make(map[string]struct{}, len(cfg.volumes))
	for _, v := range cfg.volumes {
		fi, err := os.Stat(v)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("storage node: not directory %s", v)
		}
		if err := fputil.IsWritableDir(v); err != nil {
			return err
		}

		volumes[v] = struct{}{}
	}
	if len(cfg.volumes) != len(volumes) {
		return fmt.Errorf("storage node: duplicated volumes %+v", cfg.volumes)
	}
	return nil
}

func (cfg *config) ensureVolumes() error {
	volumes := make([]string, len(cfg.volumes))
	for i := range cfg.volumes {
		absVol, err := filepath.Abs(cfg.volumes[i])
		if err != nil {
			return err
		}
		volumes[i] = absVol
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

func WithGRPCServerReadBufferSize(grpcServerReadBufferSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.grpcServerReadBufferSize = grpcServerReadBufferSize
	})
}

func WithGRPCServerWriteBufferSize(grpcServerWriteBufferSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.grpcServerWriteBufferSize = grpcServerWriteBufferSize
	})
}

func WithGRPCServerMaxRecvMsgSize(grpcServerMaxRecvMsgSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.grpcServerMaxRecvMsgSize = grpcServerMaxRecvMsgSize
	})
}

func WithReplicateClientReadBufferSize(replicateClientReadBufferSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicateClientReadBufferSize = replicateClientReadBufferSize
	})
}

func WithReplicateClientWriteBufferSize(replicateClientWriteBufferSize int64) Option {
	return newFuncOption(func(cfg *config) {
		cfg.replicateClientWriteBufferSize = replicateClientWriteBufferSize
	})
}

func WithDefaultLogStreamExecutorOptions(defaultLSEOptions ...logstream.ExecutorOption) Option {
	return newFuncOption(func(cfg *config) {
		cfg.defaultLogStreamExecutorOptions = defaultLSEOptions
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
