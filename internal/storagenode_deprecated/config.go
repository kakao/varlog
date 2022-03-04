package storagenode_deprecated

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/executor"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/pprof"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/storage"
	"github.daumkakao.com/varlog/varlog/internal/storagenode_deprecated/volume"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
)

const (
	// DefaultListenAddress is a default address to listen to incomming RPC connections.
	DefaultListenAddress = "0.0.0.0:9091"

	DefaultBallastSize = 1 << 30

	DefaultServerReadBufferSize = 32 * 1024

	DefaultServerWriteBufferSize = 32 * 1024

	DefaultReplicationClientReadBufferSize = 32 * 1024

	DefaultReplicationClientWriteBufferSize = 32 * 1024
)

type config struct {
	clusterID             types.ClusterID
	storageNodeID         types.StorageNodeID
	listenAddress         string
	advertiseAddress      string
	volumes               set.Set // set[Volume]
	executorOpts          []executor.Option
	storageOpts           []storage.Option
	pprofOpts             []pprof.Option
	serverReadBufferSize  int
	serverWriteBufferSize int
	logger                *zap.Logger
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

func (c config) validate() error {
	if c.volumes.Size() == 0 {
		return errors.Wrap(verrors.ErrInvalid, "no volumes")
	}
	if c.serverReadBufferSize < 0 {
		return errors.Wrap(verrors.ErrInvalid, "invalid serverReadBufferSize")
	}
	if c.serverWriteBufferSize < 0 {
		return errors.Wrap(verrors.ErrInvalid, "invalid serverWriteBufferSize")
	}
	return nil
}

// Option is an interface for applying options for StorageNode.
type Option interface {
	apply(*config)
}

type clusterIDOption types.ClusterID

func (o clusterIDOption) apply(c *config) {
	c.clusterID = types.ClusterID(o)
}

// WithClusterID sets the ClusterID.
func WithClusterID(clusterID types.ClusterID) Option {
	return clusterIDOption(clusterID)
}

type storageNodeIDOption types.StorageNodeID

func (o storageNodeIDOption) apply(c *config) {
	c.storageNodeID = types.StorageNodeID(o)
}

// WithStorageNodeID sets the StorageNodeID.
func WithStorageNodeID(storageNodeID types.StorageNodeID) Option {
	return storageNodeIDOption(storageNodeID)
}

type listenAddressOption string

func (o listenAddressOption) apply(c *config) {
	c.listenAddress = string(o)
}

// WithListenAddress sets the address to listen to incoming RPC connections.
func WithListenAddress(address string) Option {
	return listenAddressOption(address)
}

type advertiseAddressOption string

func (o advertiseAddressOption) apply(c *config) {
	c.advertiseAddress = string(o)
}

// WithAdvertiseAddress sets the advertising address.
// If it is not set, the listen address will be used.
func WithAdvertiseAddress(address string) Option {
	return advertiseAddressOption(address)
}

type lseOptions struct {
	opts []executor.Option
}

func (o lseOptions) apply(c *config) {
	c.executorOpts = o.opts
}

// WithExecutorOptions sets options for executor.
func WithExecutorOptions(opts ...executor.Option) Option {
	return lseOptions{opts: opts}
}

type storageOptions struct {
	opts []storage.Option
}

func (o storageOptions) apply(c *config) {
	c.storageOpts = o.opts
}

// WithStorageOptions sets options for storage.
func WithStorageOptions(opts ...storage.Option) Option {
	return storageOptions{opts: opts}
}

type volumesOption struct {
	volumes set.Set // set[Volume]
}

func (o volumesOption) apply(c *config) {
	c.volumes = o.volumes
}

// WithVolumes sets root paths to store data of the StorageNode.
// Note that it overwrites a new list of volumes rather than expanding an already defined list.
// If one of the given volumes is invalid, it panics.
func WithVolumes(dirs ...string) Option {
	volumes := set.New(len(dirs))
	for _, dir := range dirs {
		vol, err := volume.New(dir)
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

// WithPProfOptions sets options for pprof.
func WithPProfOptions(opts ...pprof.Option) Option {
	return pprofOptions{opts}
}

type serverConfig struct {
	storageNode *StorageNode
	logger      *zap.Logger
}

type serverReadBufferSizeOption int

func (o serverReadBufferSizeOption) apply(c *config) {
	c.serverReadBufferSize = int(o)
}

func WithServerReadBufferSize(readBufferSize int) Option {
	return serverReadBufferSizeOption(readBufferSize)
}

type serverWriteBufferSizeOption int

func (o serverWriteBufferSizeOption) apply(c *config) {
	c.serverWriteBufferSize = int(o)
}

func WithServerWriteBufferSize(writeBufferSize int) Option {
	return serverWriteBufferSizeOption(writeBufferSize)
}

func newServerConfig(opts []serverOption) serverConfig {
	cfg := serverConfig{
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
	if c.storageNode == nil {
		return errors.New("no storage node configured")
	}
	if c.logger == nil {
		return errors.New("no logger configured")
	}
	return nil
}

type serverOption interface {
	applyServer(*serverConfig)
}

// LoggerOption is an interface for applying options for logger.
type LoggerOption interface {
	Option
	serverOption
}

type loggerOption struct {
	logger *zap.Logger
}

func (o loggerOption) apply(c *config) {
	c.logger = o.logger
}

func (o loggerOption) applyServer(c *serverConfig) {
	c.logger = o.logger
}

// WithLogger sets logger.
func WithLogger(logger *zap.Logger) LoggerOption {
	return loggerOption{logger}
}

type storageNodeOption struct {
	sn *StorageNode
}

func (o storageNodeOption) applyServer(c *serverConfig) {
	c.storageNode = o.sn
}

func withStorageNode(sn *StorageNode) serverOption {
	return storageNodeOption{sn}
}
