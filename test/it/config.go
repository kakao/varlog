package it

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/vms"
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

	defaultNumSN = 0
	defaultNumLS = 0
	defaultNumCL = 0

	defaultVMSPortOffset = ports.ReservationSize - 1
	defaultStartVMS      = true
)

func defaultReportClientFactory() metadata_repository.ReporterClientFactory {
	return metadata_repository.NewReporterClientFactory()
}

func defaultStorageNodeManagementClientFactory() metadata_repository.StorageNodeManagementClientFactory {
	return metadata_repository.NewStorageNodeManagementClientFactory()
}

type config struct {
	clusterID             types.ClusterID
	nrRep                 int
	nrMR                  int
	snapCount             int
	collectorName         string
	unsafeNoWAL           bool
	reporterClientFac     metadata_repository.ReporterClientFactory
	snManagementClientFac metadata_repository.StorageNodeManagementClientFactory

	numSN int
	numLS int
	numCL int

	VMSOpts *vms.Options
	logger  *zap.Logger

	portBase      int
	vmsPortOffset int

	startVMS bool
}

func newConfig(t *testing.T, opts []Option) config {
	cfg := config{
		nrRep:                 defaultReplicationFactor,
		nrMR:                  defaultMRCount,
		snapCount:             defaultSnapCount,
		unsafeNoWAL:           defaultUnsafeNoWAL,
		reporterClientFac:     defaultReportClientFactory(),
		snManagementClientFac: defaultStorageNodeManagementClientFactory(),
		VMSOpts:               NewTestVMSOptions(),
		logger:                zap.NewNop(),
		portBase:              defaultPortBase,
		vmsPortOffset:         defaultVMSPortOffset,
		startVMS:              defaultStartVMS,
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
	require.NotNil(t, c.snManagementClientFac)
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

func WithReporterClientFactory(fac metadata_repository.ReporterClientFactory) Option {
	return func(c *config) {
		c.reporterClientFac = fac
	}
}

func WithStorageNodeManagementClientFactory(fac metadata_repository.StorageNodeManagementClientFactory) Option {
	return func(c *config) {
		c.snManagementClientFac = fac
	}
}

func WithVMSOptions(vmsOpts *vms.Options) Option {
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

func NewTestVMSOptions() *vms.Options {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.Tick = 100 * time.Millisecond
	vmsOpts.HeartbeatTimeout = 30
	vmsOpts.ReportInterval = 10
	vmsOpts.Logger = zap.L()
	return &vmsOpts
}
