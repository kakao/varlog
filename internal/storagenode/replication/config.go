package replication

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode/id"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	DefaultRequestQueueSize = 1024
	DefaultPipelineSize     = 1
)

type clientConfig struct {
	replica          varlogpb.Replica
	requestQueueSize int
	grpcDialOptions  []grpc.DialOption
	metrics          *telemetry.Metrics
	logger           *zap.Logger
}

func newClientConfig(opts []ClientOption) (*clientConfig, error) {
	cfg := &clientConfig{
		requestQueueSize: DefaultRequestQueueSize,
		logger:           zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyClient(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c clientConfig) validate() error {
	// TODO: more meaningful check
	if len(c.replica.Address) == 0 {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.requestQueueSize <= 0 {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.metrics == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if c.logger == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return nil
}

type serverConfig struct {
	storageNodeIDGetter id.StorageNodeIDGetter
	pipelineQueueSize   int
	logReplicatorGetter Getter
	metrics             *telemetry.Metrics
	logger              *zap.Logger
}

func newServerConfig(opts []ServerOption) serverConfig {
	cfg := serverConfig{
		pipelineQueueSize: DefaultPipelineSize,
		logger:            zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyServer(&cfg)
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

func (s serverConfig) validate() error {
	if s.storageNodeIDGetter == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if s.pipelineQueueSize < 0 {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if s.logReplicatorGetter == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if s.metrics == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	if s.logger == nil {
		return errors.WithStack(verrors.ErrInvalid)
	}
	return nil
}

type connectorConfig struct {
	clientOptions []ClientOption
	logger        *zap.Logger
}

func newConnectorConfig(opts []ConnectorOption) (*connectorConfig, error) {
	cfg := &connectorConfig{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyConnector(cfg)
	}
	if cfg.logger == nil {
		return nil, errors.WithStack(verrors.ErrInvalid)
	}
	return cfg, nil
}

type ClientOption interface {
	applyClient(*clientConfig)
}

type ServerOption interface {
	applyServer(*serverConfig)
}

type ConnectorOption interface {
	applyConnector(*connectorConfig)
}

type replicaOption varlogpb.Replica

func (o replicaOption) applyClient(c *clientConfig) {
	c.replica = varlogpb.Replica(o)
}

func WithReplica(replica varlogpb.Replica) ClientOption {
	return replicaOption(replica)
}

type requestQueueSizeOption int

func (o requestQueueSizeOption) applyClient(c *clientConfig) {
	c.requestQueueSize = int(o)
}

func WithRequestQueueSize(queueSize int) ClientOption {
	return requestQueueSizeOption(queueSize)
}

type grpcDialOpt struct {
	grpcDialOptions []grpc.DialOption
}

func (o grpcDialOpt) applyClient(c *clientConfig) {
	c.grpcDialOptions = append(c.grpcDialOptions, o.grpcDialOptions...)
}

func WithGRPCDialOptions(grpcDialOptions ...grpc.DialOption) ClientOption {
	return grpcDialOpt{
		grpcDialOptions: grpcDialOptions,
	}
}

type LoggerOption interface {
	ServerOption
	ClientOption
	ConnectorOption
}

type loggerOption struct {
	logger *zap.Logger
}

func (o loggerOption) applyClient(c *clientConfig) {
	c.logger = o.logger
}

func (o loggerOption) applyServer(c *serverConfig) {
	c.logger = o.logger
}

func (o loggerOption) applyConnector(c *connectorConfig) {
	c.logger = o.logger
}

func WithLogger(logger *zap.Logger) LoggerOption {
	return loggerOption{logger}
}

type pipelineQueueSizeOption int

func (o pipelineQueueSizeOption) applyServer(c *serverConfig) {
	c.pipelineQueueSize = int(o)
}

func WithPipelineQueueSize(sz int) ServerOption {
	return pipelineQueueSizeOption(sz)
}

type logReplicatorGetterOption struct {
	getter Getter
}

func (o logReplicatorGetterOption) applyServer(c *serverConfig) {
	c.logReplicatorGetter = o.getter
}

func WithLogReplicatorGetter(getter Getter) ServerOption {
	return logReplicatorGetterOption{getter}
}

type defaultClientOptions struct {
	opts []ClientOption
}

func (o defaultClientOptions) applyConnector(c *connectorConfig) {
	c.clientOptions = o.opts
}

func WithClientOptions(opts ...ClientOption) ConnectorOption {
	return defaultClientOptions{opts}
}

type snidGetterOption struct {
	getter id.StorageNodeIDGetter
}

func (o snidGetterOption) applyServer(c *serverConfig) {
	c.storageNodeIDGetter = o.getter
}

func WithStorageNodeIDGetter(snidGetter id.StorageNodeIDGetter) ServerOption {
	return snidGetterOption{snidGetter}
}

type MeasurableOption interface {
	ServerOption
	ClientOption
}

type measurableOption struct {
	m *telemetry.Metrics
}

func (o measurableOption) applyServer(c *serverConfig) {
	c.metrics = o.m
}

func (o measurableOption) applyClient(c *clientConfig) {
	c.metrics = o.m
}

func WithMetrics(measurable *telemetry.Metrics) MeasurableOption {
	return measurableOption{measurable}
}
