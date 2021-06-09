package benchmark

import (
	"errors"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
)

const (
	DefaultNumClients        = 5
	DefaultDataSizeByte      = 1024
	DefaultReportIntervalOps = 100

	DefaultMaxOperationsPerClient = 1000
	DefaultMaxDuration            = 10 * time.Minute
)

type config struct {
	clusterID                   types.ClusterID
	metadataRepositoryAddresses []string
	numClients                  int
	varlogOpts                  []varlog.Option
	reportIntervalOps           int
	dataSizeByte                int64

	maxOpsPerClient int
	maxDuration     time.Duration
}

func newConfig(opts []Option) config {
	cfg := config{
		clusterID:         1,
		numClients:        DefaultNumClients,
		reportIntervalOps: DefaultReportIntervalOps,
		dataSizeByte:      DefaultDataSizeByte,
		maxOpsPerClient:   DefaultMaxOperationsPerClient,
		maxDuration:       DefaultMaxDuration,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		panic(err)
	}
	return cfg
}

func (c config) validate() error {
	if len(c.metadataRepositoryAddresses) == 0 {
		return errors.New("no metadata repository address")
	}
	if c.numClients < 1 {
		return errors.New("no client")
	}
	return nil
}

type Option func(*config)

func WithClusterID(cid types.ClusterID) Option {
	return func(c *config) {
		c.clusterID = cid
	}
}

func WithMetadataRepositoryAddresses(addrs ...string) Option {
	return func(c *config) {
		c.metadataRepositoryAddresses = addrs
	}
}

func WithNumberOfClients(num int) Option {
	return func(c *config) {
		c.numClients = num
	}
}

func WithVarlogOptions(opts ...varlog.Option) Option {
	return func(c *config) {
		c.varlogOpts = opts
	}
}

func WithReportIntervalInOperations(ops int) Option {
	return func(c *config) {
		c.reportIntervalOps = ops
	}
}

func WithDataSizeInBytes(size int64) Option {
	return func(c *config) {
		c.dataSizeByte = size
	}
}

func WithMaxOperationsPerClient(ops int) Option {
	return func(c *config) {
		c.maxOpsPerClient = ops
	}
}

func WithMaxDuration(dur time.Duration) Option {
	return func(c *config) {
		c.maxDuration = dur
	}
}
