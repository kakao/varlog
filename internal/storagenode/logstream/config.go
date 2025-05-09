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
	DefaultSyncTimeout                  = 10 * time.Second
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
	syncTimeout                  time.Duration

	// maxWriteTaskBatchLength sets how many tasks are batched together in a
	// writer or backupWriter. Each task may contain multiple writes (as
	// [][]byte) to the storage layer.
	//
	// If each task has N writes and maxWriteTaskBatchLength is M, then up to
	// N*M writes can be flushed to storage at once when M or more tasks are
	// queued. To control the number of writes sent to storage, M is limited
	// from 1 to 3.
	maxWriteTaskBatchLength int
}

func newExecutorConfig(opts []ExecutorOption) (executorConfig, error) {
	cfg := executorConfig{
		sequenceQueueCapacity:        DefaultSequenceQueueCapacity,
		writeQueueCapacity:           DefaultWriteQueueCapacity,
		commitQueueCapacity:          DefaultCommitQueueCapacity,
		replicateClientQueueCapacity: DefaultReplicateClientQueueCapacity,
		logger:                       zap.NewNop(),
		syncTimeout:                  DefaultSyncTimeout,
	}
	for _, opt := range opts {
		opt.applyExecutor(&cfg)
	}
	cfg.ensureDefaults()
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

func (cfg *executorConfig) ensureDefaults() {
	cfg.maxWriteTaskBatchLength = min(max(cfg.maxWriteTaskBatchLength, 1), 3)
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

// WithSyncTimeout sets timeout for synchronization in the destination replica.
// If the destination replica doesn't receive the SyncReplicate RPC within
// syncTimeout, other SyncInit RPC can cancel the synchronization.
func WithSyncTimeout(syncTimeout time.Duration) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.syncTimeout = syncTimeout
	})
}

// WithMaxWriteTaskBatchLength sets the maximum number of write tasks that can
// be grouped together to optimize write operations by reducing overhead. Each
// write task may already batch multiple writes, so the number of grouped tasks
// is capped at 3 for efficiency.
// If the provided value is zero or negative, it defaults to 1. If the value is
// 4 or greater, it defaults to 3.
func WithMaxWriteTaskBatchLength(maxWriteBatchLength int) ExecutorOption {
	return newFuncExecutorOption(func(cfg *executorConfig) {
		cfg.maxWriteTaskBatchLength = maxWriteBatchLength
	})
}
