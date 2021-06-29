package storagenode

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/kakao/varlog/internal/storagenode/executor"
	"github.com/kakao/varlog/internal/storagenode/pprof"
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/verrors"
)

const (
	DefaultListenAddress = "0.0.0.0:9091"
)

type config struct {
	cid types.ClusterID

	snid types.StorageNodeID

	listenAddress string

	advertiseAddress string

	telemetryEndpoint string

	volumes set.Set // set[Volume]

	executorOpts []executor.Option

	storageOpts []storage.Option

	pprofOpts []pprof.Option

	logger *zap.Logger
}

func newConfig(opts []Option) (*config, error) {
	cfg := &config{
		listenAddress: DefaultListenAddress,
		logger:        zap.NewNop(),
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// TODO
func (c config) validate() error {
	if c.volumes.Size() == 0 {
		return errors.Wrap(verrors.ErrInvalid, "no volumes")
	}
	return nil
}

type Option interface {
	apply(*config)
}

type clusterIDOption types.ClusterID

func (o clusterIDOption) apply(c *config) {
	c.cid = types.ClusterID(o)
}

func WithClusterID(clusterID types.ClusterID) Option {
	return clusterIDOption(clusterID)
}

type storageNodeIDOption types.StorageNodeID

func (o storageNodeIDOption) apply(c *config) {
	c.snid = types.StorageNodeID(o)
}

func WithStorageNodeID(storageNodeID types.StorageNodeID) Option {
	return storageNodeIDOption(storageNodeID)
}

type listenAddressOption string

func (o listenAddressOption) apply(c *config) {
	c.listenAddress = string(o)
}

func WithListenAddress(address string) Option {
	return listenAddressOption(address)
}

type advertiseAddressOption string

func (o advertiseAddressOption) apply(c *config) {
	c.advertiseAddress = string(o)
}

func WithAdvertiseAddress(address string) Option {
	return advertiseAddressOption(address)
}

type lseOptions struct {
	opts []executor.Option
}

func (o lseOptions) apply(c *config) {
	c.executorOpts = o.opts
}

func WithExecutorOptions(opts ...executor.Option) Option {
	return lseOptions{opts: opts}
}

type storageOptions struct {
	opts []storage.Option
}

func (o storageOptions) apply(c *config) {
	c.storageOpts = o.opts
}

func WithStorageOptions(opts ...storage.Option) Option {
	return storageOptions{opts: opts}
}

type volumesOption struct {
	volumes set.Set // set[Volume]
}

func (o volumesOption) apply(c *config) {
	c.volumes = o.volumes
}

func WithVolumes(dirs ...string) Option {
	volumes := set.New(len(dirs))
	for _, dir := range dirs {
		vol, err := NewVolume(dir)
		if err != nil {
			panic(err)
		}
		volumes.Add(vol)
	}
	return volumesOption{volumes}
}

type pprofOptions struct {
	opts []pprof.Option
}

func (o pprofOptions) apply(c *config) {
	c.pprofOpts = o.opts
}

func WithPProfOptions(opts ...pprof.Option) Option {
	return pprofOptions{opts}
}

type telemetryOption string

func (o telemetryOption) apply(c *config) {
	c.telemetryEndpoint = string(o)
}

func WithTelemetry(endpoint string) Option {
	return telemetryOption(endpoint)
}

type serverConfig struct {
	storageNode *StorageNode
	tmStub      *telemetry.TelemetryStub
	logger      *zap.Logger
}

func newServerConfig(opts []ServerOption) serverConfig {
	cfg := serverConfig{
		tmStub: telemetry.NewNopTelmetryStub(),
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt.applyServer(&cfg)
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

func (c serverConfig) validate() error {
	return nil
}

type ServerOption interface {
	applyServer(*serverConfig)
}

type loggerOption struct {
	logger *zap.Logger
}

func (o loggerOption) apply(c *config) {
	c.logger = o.logger
}

func WithLogger(logger *zap.Logger) Option {
	return loggerOption{logger}
}

type storageNodeOption struct {
	sn *StorageNode
}

func (o storageNodeOption) applyServer(c *serverConfig) {
	c.storageNode = o.sn
}

func WithStorageNode(sn *StorageNode) ServerOption {
	return storageNodeOption{sn}
}
