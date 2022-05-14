package it

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/metarepos"
	"github.daumkakao.com/varlog/varlog/internal/varlogadm"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil/ports"
)

const (
	defaultClusterID         = types.ClusterID(1)
	defaultReplicationFactor = 1
	defaultMRCount           = 1
	defaultSnapCount         = 0
	defaultUnsafeNoWAL       = false
	defaultPortBase          = 10000

	defaultVMSPortOffset = ports.ReservationSize - 1
	defaultStartVMS      = true
)

func defaultReportClientFactory() metarepos.ReporterClientFactory {
	return metarepos.NewReporterClientFactory()
}

type config struct {
	clusterID         types.ClusterID
	nrRep             int
	nrMR              int
	snapCount         int
	collectorName     string
	unsafeNoWAL       bool
	reporterClientFac metarepos.ReporterClientFactory

	numSN    int
	numLS    int
	numCL    int
	numTopic int

	VMSOpts []varlogadm.Option
	logger  *zap.Logger

	portBase      int
	vmsPortOffset int

	startVMS bool
}

func newConfig(t *testing.T, opts []Option) config {
	cfg := config{
		nrRep:             defaultReplicationFactor,
		nrMR:              defaultMRCount,
		snapCount:         defaultSnapCount,
		unsafeNoWAL:       defaultUnsafeNoWAL,
		reporterClientFac: defaultReportClientFactory(),
		VMSOpts:           NewTestVMSOptions(),
		logger:            zap.NewNop(),
		portBase:          defaultPortBase,
		vmsPortOffset:     defaultVMSPortOffset,
		startVMS:          defaultStartVMS,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	cfg.validate(t)
	return cfg
}

func (c *config) validate(t *testing.T) {
	require.Positive(t, c.nrRep)
	require.GreaterOrEqual(t, c.nrMR, 0)
	require.GreaterOrEqual(t, c.snapCount, 0)
	require.NotNil(t, c.reporterClientFac)
	require.NotNil(t, c.VMSOpts)
	require.GreaterOrEqual(t, c.numSN, 0)
	require.GreaterOrEqual(t, c.numLS, 0)
	require.GreaterOrEqual(t, c.numCL, 0)
	require.NotNil(t, c.logger)
}

type Option func(*config)

func WithClusterID(cid types.ClusterID) Option {
	return func(c *config) {
		c.clusterID = cid
	}
}

func WithReplicationFactor(rf int) Option {
	return func(c *config) {
		c.nrRep = rf
	}
}

func WithMRCount(num int) Option {
	return func(c *config) {
		c.nrMR = num
	}
}

func WithSnapCount(cnt int) Option {
	return func(c *config) {
		c.snapCount = cnt
	}
}

func WithoutWAL() Option {
	return func(c *config) {
		c.unsafeNoWAL = true
	}
}

func WithCollectorName(collector string) Option {
	return func(c *config) {
		c.collectorName = collector
	}
}

func WithReporterClientFactory(fac metarepos.ReporterClientFactory) Option {
	return func(c *config) {
		c.reporterClientFac = fac
	}
}

func WithVMSOptions(vmsOpts []varlogadm.Option) Option {
	return func(c *config) {
		c.VMSOpts = vmsOpts
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func WithPortBase(portBase int) Option {
	return func(c *config) {
		c.portBase = portBase
	}
}

func WithNumberOfStorageNodes(numSN int) Option {
	return func(c *config) {
		c.numSN = numSN
	}
}

func WithNumberOfLogStreams(numLS int) Option {
	return func(c *config) {
		c.numLS = numLS
	}
}

func WithNumberOfTopics(numTopic int) Option {
	return func(c *config) {
		c.numTopic = numTopic
	}
}

func WithNumberOfClients(numCL int) Option {
	return func(c *config) {
		c.numCL = numCL
	}
}

func WithoutVMS() Option {
	return func(c *config) {
		c.startVMS = false
	}
}

func NewTestVMSOptions(opts ...varlogadm.Option) []varlogadm.Option {
	ret := []varlogadm.Option{
		varlogadm.WithWatcherOptions(
			varlogadm.WithWatcherTick(100*time.Millisecond),
			varlogadm.WithWatcherHeartbeatTimeout(30),
			varlogadm.WithWatcherReportInterval(10),
		),
		varlogadm.WithLogger(zap.L()),
	}
	ret = append(ret, opts...)
	return ret
}
