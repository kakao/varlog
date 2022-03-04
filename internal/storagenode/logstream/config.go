package logstream

import (
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultSequenceQueueCapacity        = 1024
	DefaultWriteQueueCapacity           = 1024
	DefaultCommitQueueCapacity          = 1024
	DefaultReplicateClientQueueCapacity = 1024
	DefaultSyncInitTimeout              = 10 * time.Second
)

type executorConfig struct {
	cid                          types.ClusterID
	snid                         types.StorageNodeID
	tpid                         types.TopicID
	lsid                         types.LogStreamID
	advertiseAddress             string
	stg                          *storage.Storage
	sequenceQueueCapacity        int
	writeQueueCapacity           int
	commitQueueCapacity          int
	replicateClientQueueCapacity int
	replicateClientGRPCOptions   []grpc.DialOption
	logger                       *zap.Logger
	lsm                          *telemetry.LogStreamMetrics
	syncInitTimeout              time.Duration
}

func newExecutorConfig(opts []ExecutorOption) (executorConfig, error) {
	cfg := executorConfig{
		sequenceQueueCapacity:        DefaultSequenceQueueCapacity,
		writeQueueCapacity:           DefaultWriteQueueCapacity,
		commitQueueCapacity:          DefaultCommitQueueCapacity,
		replicateClientQueueCapacity: DefaultReplicateClientQueueCapacity,
		logger:                       zap.NewNop(),
		syncInitTimeout:              DefaultSyncInitTimeout,
	}
	for _, opt := range opts {
		opt.applyExecutor(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	cfg.logger = cfg.logger.Named("lse").With(
		zap.Int32("tpid", int32(cfg.tpid)),
		zap.Int32("lsid", int32(cfg.lsid)),
	)
	return cfg, nil
}

func (cfg executorConfig) validate() error {
	if err := validateQueueCapacity("sequence", cfg.sequenceQueueCapacity); err != nil {
		return err
	}
	if err := validateQueueCapacity("write", cfg.writeQueueCapacity); err != nil {
		return err
	}
	if err := validateQueueCapacity("commit", cfg.commitQueueCapacity); err != nil {
		return err
	}
	if err := validateQueueCapacity("replicate client", cfg.replicateClientQueueCapacity); err != nil {
		return err
	}
	if cfg.stg == nil {
		return errStorageIsNil
	}
	if cfg.logger == nil {
		return errLoggerIsNil
	}
	return nil
}

type ExecutorOption interface {
	applyExecutor(cfg *executorConfig)
}

type funcExecutorOption struct {
	f func(config *executorConfig)
}

func newFuncExecutorOption(f func(*executorConfig)) *funcExecutorOption {
	return &funcExecutorOption{f: f}
}

func (feo *funcExecutorOption) applyExecutor(cfg *executorConfig) {
	feo.f(cfg)
}

func WithClusterID(cid types.ClusterID) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.cid = cid
	})
}

func WithStorageNodeID(snid types.StorageNodeID) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.snid = snid
	})
}

func WithTopicID(tpid types.TopicID) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.tpid = tpid
	})
}

func WithLogStreamID(lsid types.LogStreamID) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.lsid = lsid
	})
}

func WithAdvertiseAddress(advertiseAddress string) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.advertiseAddress = advertiseAddress
	})
}

func WithStorage(stg *storage.Storage) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.stg = stg
	})
}

func WithSequenceQueueCapacity(sequenceQueueCapacity int) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.sequenceQueueCapacity = sequenceQueueCapacity
	})
}

func WithWriteQueueCapacity(writeQueueCapacity int) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.writeQueueCapacity = writeQueueCapacity
	})
}

func WithCommitQueueCapacity(commitQueueCapacity int) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.commitQueueCapacity = commitQueueCapacity
	})
}

func WithReplicateClientQueueCapacity(replicateClientQueueCapacity int) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.replicateClientQueueCapacity = replicateClientQueueCapacity
	})
}

func WithReplicateClientGRPCOptions(replicateClientGRPCOptions ...grpc.DialOption) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.replicateClientGRPCOptions = replicateClientGRPCOptions
	})
}

func WithLogger(logger *zap.Logger) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.logger = logger
	})
}

func WithLogStreamMetrics(lsm *telemetry.LogStreamMetrics) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.lsm = lsm
	})
}

func WithSyncInitTimeout(syncInitTimeout time.Duration) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.syncInitTimeout = syncInitTimeout
	})
}
