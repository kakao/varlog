package executor

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode_deprecated/storage"
	"github.com/kakao/varlog/internal/storagenode_deprecated/telemetry"
	"github.com/kakao/varlog/pkg/types"
)

const (
	DefaultWriteQueueSize = 1024
	DefaultWriteBatchSize = 1024

	DefaultCommitQueueSize     = 1024
	DefaultCommitTaskQueueSize = 1024
	DefaultCommitBatchSize     = 1024

	DefaultReplicateQueueSize = 1024
)

type config struct {
	storageNodeID types.StorageNodeID
	logStreamID   types.LogStreamID
	topicID       types.TopicID
	storage       storage.Storage

	writeQueueSize int
	writeBatchSize int

	commitQueueSize     int
	commitTaskQueueSize int
	commitBatchSize     int

	replicateQueueSize               int
	replicationClientReadBufferSize  int
	replicationClientWriteBufferSize int

	metrics *telemetry.Metrics
	logger  *zap.Logger
}

func (c config) validate() error {
	if c.storage == nil {
		return errors.New("storage: nil")
	}
	if c.writeQueueSize <= 0 {
		return errors.New("write queue size: negative or zero")
	}
	if c.writeBatchSize <= 0 {
		return errors.New("batch size: negative or zero")
	}
	if c.commitQueueSize <= 0 {
		return errors.New("commit queue size: negative or zero")
	}
	if c.commitTaskQueueSize <= 0 {
		return errors.New("commit task queue size: negative or zero")
	}
	if c.commitBatchSize <= 0 {
		return errors.New("committer batch size: negative or zero")
	}
	if c.replicateQueueSize <= 0 {
		return errors.New("replicate queue size: negative or zero")
	}
	if c.metrics == nil {
		return errors.New("no measurable")
	}
	if c.logger == nil {
		return errors.New("logger: nil")
	}
	return nil
}

// Option is an interface for applying options for executor.
type Option interface {
	apply(*config)
}

func newConfig(opts []Option) (*config, error) {
	cfg := &config{
		writeQueueSize:      DefaultWriteQueueSize,
		writeBatchSize:      DefaultWriteBatchSize,
		commitQueueSize:     DefaultCommitQueueSize,
		commitTaskQueueSize: DefaultCommitTaskQueueSize,
		commitBatchSize:     DefaultCommitBatchSize,
		replicateQueueSize:  DefaultReplicateQueueSize,
		logger:              zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

type storageNodeIDOption types.StorageNodeID

func (o storageNodeIDOption) apply(c *config) {
	c.storageNodeID = types.StorageNodeID(o)
}

func WithStorageNodeID(snid types.StorageNodeID) Option {
	return storageNodeIDOption(snid)
}

type logStreamIDOption types.LogStreamID

func (o logStreamIDOption) apply(c *config) {
	c.logStreamID = types.LogStreamID(o)
}

func WithLogStreamID(lsid types.LogStreamID) Option {
	return logStreamIDOption(lsid)
}

type topicIDOption types.TopicID

func (o topicIDOption) apply(c *config) {
	c.topicID = types.TopicID(o)
}

func WithTopicID(topicID types.TopicID) Option {
	return topicIDOption(topicID)
}

/*
type StorageOption interface {
	WriterOption
}
*/

type storageOption struct {
	storage storage.Storage
}

func (o storageOption) apply(c *config) {
	c.storage = o.storage
}

/*
func (o storageOption) applyWriter(c *writerConfig) {
	c.strg = o.storage
}
*/

func WithStorage(storage storage.Storage) Option {
	return storageOption{storage: storage}
}

/*
type QueueSizeOption interface {
	WriterOption
}

type queueSizeOption int

func (o queueSizeOption) applyWriter(c *writerConfig) {
	c.queueSize = int(o)
}

func WithQueueSize(queueSize int) QueueSizeOption {
	return queueSizeOption(queueSize)
}

type BatchSizeOption interface {
	WriterOption
}

type batchSizeOption int

func (o batchSizeOption) applyWriter(c *writerConfig) {
	c.batchSize = int(o)
}

func WithBatchSize(batchSize int) BatchSizeOption {
	return batchSizeOption(batchSize)
}
*/

type writeQueueSizeOption int

func (o writeQueueSizeOption) apply(c *config) {
	c.writeQueueSize = int(o)
}

func WithWriteQueueSize(queueSize int) Option {
	return writeQueueSizeOption(queueSize)
}

type writeBatchSizeOption int

func (o writeBatchSizeOption) apply(c *config) {
	c.writeBatchSize = int(o)
}

func WithWriteBatchSize(batchSize int) Option {
	return writeBatchSizeOption(batchSize)
}

type commitQueueSizeOption int

func (o commitQueueSizeOption) apply(c *config) {
	c.commitQueueSize = int(o)
}

func WithCommitQueueSize(queueSize int) Option {
	return commitQueueSizeOption(queueSize)
}

type commitTaskQueueSizOption int

func (o commitTaskQueueSizOption) apply(c *config) {
	c.commitQueueSize = int(o)
}

func WithCommitTaskQueueSize(queueSize int) Option {
	return commitTaskQueueSizOption(queueSize)
}

type commitBatchSizeOption int

func (o commitBatchSizeOption) apply(c *config) {
	c.commitBatchSize = int(o)
}

func WithCommitBatchSize(batchSize int) Option {
	return commitBatchSizeOption(batchSize)
}

type replicateQueueSizeOption int

func (o replicateQueueSizeOption) apply(c *config) {
	c.replicateQueueSize = int(o)
}

func WithReplicateQueueSize(queueSize int) Option {
	return replicateQueueSizeOption(queueSize)
}

type replicationClientReadBufferSizeOption int

func (o replicationClientReadBufferSizeOption) apply(c *config) {
	c.replicationClientReadBufferSize = int(o)
}

func WithReplicationClientReadBufferSize(readBufferSize int) Option {
	return replicationClientReadBufferSizeOption(readBufferSize)
}

type replicationClientWriteBufferSizeOption int

func (o replicationClientWriteBufferSizeOption) apply(c *config) {
	c.replicationClientWriteBufferSize = int(o)
}

func WithReplicationClientWriteBufferSize(writeBufferSize int) Option {
	return replicationClientWriteBufferSizeOption(writeBufferSize)
}

type loggerOption struct {
	logger *zap.Logger
}

func (o loggerOption) apply(c *config) {
	c.logger = o.logger
}

func WithLogger(logger *zap.Logger) Option {
	return loggerOption{logger: logger}
}

type measurableOption struct {
	m *telemetry.Metrics
}

func (o measurableOption) apply(c *config) {
	c.metrics = o.m
}

func WithMetrics(measure *telemetry.Metrics) Option {
	return measurableOption{measure}
}

/*
type LogStreamContextOption interface {
	WriterOption
}

type logStreamContextOption struct {
	lsc *logStreamContext
}

func (o logStreamContextOption) applyWriter(c *writerConfig) {
	c.lsc = o.lsc
}

func WithLogStreamContext(lsc *logStreamContext) LogStreamContextOption {
	return logStreamContextOption{lsc}
}

type committerOption struct {
	committer *committerImpl
}

func (o committerOption) applyWriter(c *writerConfig) {
	c.committer = o.committer
}

func WithCommitter(c *committerImpl) WriterOption {
	return committerOption{c}
}

type replicatorOption struct {
	replicator *replicatorImpl
}

func (o replicatorOption) applyWriter(c *writerConfig) {
	c.replicator = o.replicator
}

func WithReplicator(r *replicatorImpl) WriterOption {
	return replicatorOption{r}
}
*/
