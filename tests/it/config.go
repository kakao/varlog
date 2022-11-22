package it

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snwatcher"
	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
)

const (
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

	mrMgrOpts []mrmanager.Option
	VMSOpts   []admin.Option
	logger    *zap.Logger

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
		logger:            zaptest.NewLogger(t),
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

func WithMetadataRepositoryManagerOptions(opts ...mrmanager.Option) Option {
	return func(c *config) {
		c.mrMgrOpts = opts
	}
}

func WithVMSOptions(vmsOpts ...admin.Option) Option {
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

func NewTestVMSOptions(opts ...admin.Option) []admin.Option {
	ret := []admin.Option{
		admin.WithStorageNodeWatcherOptions(
			snwatcher.WithTick(100*time.Millisecond),
			snwatcher.WithHeartbeatTimeout(30),
			snwatcher.WithReportInterval(10),
		),
		admin.WithLogger(zap.L()),
	}
	ret = append(ret, opts...)
	return ret
}
