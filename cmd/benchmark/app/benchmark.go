package app

import (
	"context"

	"github.com/docker/go-units"
	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/pkg/benchmark"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/mathutil"
)

func main(c *cli.Context) error {
	cid, err := types.ParseClusterID(c.String("cluster-id"))
	if err != nil {
		return err
	}
	mraddrs := c.StringSlice("metadata-repository-address")

	dataSize, err := units.FromHumanSize(c.String("data-size"))
	if err != nil {
		return err
	}

	clients := mathutil.MaxInt(benchmark.DefaultNumClients, c.Int("clients"))

	reportIntervalOps := benchmark.DefaultReportIntervalOps
	if c.IsSet("report-interval") {
		reportIntervalOps = c.Int("report-interval")
	}

	maxOpsPerClient := benchmark.DefaultMaxOperationsPerClient
	if c.IsSet("max-operations-per-client") {
		maxOpsPerClient = c.Int("max-operations-per-client")
	}

	maxDuration := benchmark.DefaultMaxDuration
	if c.IsSet("max-duration") {
		maxDuration = c.Duration("max-duration")
	}

	b, err := benchmark.New(
		context.TODO(),
		benchmark.WithClusterID(cid),
		benchmark.WithMetadataRepositoryAddresses(mraddrs...),
		benchmark.WithNumberOfClients(clients),
		benchmark.WithDataSizeInBytes(dataSize),
		benchmark.WithReportIntervalInOperations(reportIntervalOps),
		benchmark.WithMaxOperationsPerClient(maxOpsPerClient),
		benchmark.WithMaxDuration(maxDuration),
	)
	if err != nil {
		return err
	}
	defer func() {
		_ = b.Close()
	}()

	return b.Serve()
}
