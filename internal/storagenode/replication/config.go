package replication

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/id"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

const (
	DefaultRequestQueueSize = 1024
	DefaultPipelineSize     = 1
)

type clientConfig struct {
	replica          snpb.Replica
	requestQueueSize int
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
	return nil
}

type serverConfig struct {
	storageNodeIDGetter id.StorageNodeIDGetter
	pipelineQueueSize   int
	logReplicatorGetter Getter
	tmStub              *telemetry.TelemetryStub
	logger              *zap.Logger
}

func newServerConfig(opts []ServerOption) serverConfig {
	cfg := serverConfig{
		pipelineQueueSize: DefaultPipelineSize,
		tmStub:            telemetry.NewNopTelmetryStub(),
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
	if s.tmStub == nil {
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

type replicaOption snpb.Replica

func (o replicaOption) applyClient(c *clientConfig) {
	c.replica = snpb.Replica(o)
}

func WithReplica(replica snpb.Replica) ClientOption {
	return replicaOption(replica)
}

type requestQueueSizeOption int

func (o requestQueueSizeOption) applyClient(c *clientConfig) {
	c.requestQueueSize = int(o)
}

func WithRequestQueueSize(queueSize int) ClientOption {
	return requestQueueSizeOption(queueSize)
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

type telemetryOption struct {
	t *telemetry.TelemetryStub
}

func (o telemetryOption) applyServer(c *serverConfig) {
	c.tmStub = o.t
}

func WithTelemetry(t *telemetry.TelemetryStub) ServerOption {
	return telemetryOption{t: t}
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
