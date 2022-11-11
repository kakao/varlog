package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/pkg/types"
)

func main() {
	os.Exit(run())
}

func run() int {
	app := newApp()
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%+v", err)
		return -1
	}
	return 0
}

func newApp() *cli.App {
	app := &cli.App{
		Name: "benchmark",
		Flags: []cli.Flag{
			flagClusterID.StringFlag(false, benchmark.DefaultClusterID.String()),
			flagTopicID.StringFlag(true, ""),
			flagLogStreamID.StringFlag(true, ""),
			flagMRAddrs.StringSliceFlag(true, nil),
			flagMsgSize.IntFlag(true, 0),
			flagBatchSize.IntFlag(false, benchmark.DefaultBatchSize),
			flagConcurrency.IntFlag(false, benchmark.DefaultConcurrency),
			flagDuration.DurationFlag(false, benchmark.DefaultDuration),
			flagReportInterval.DurationFlag(false, benchmark.DefaultReportInterval),
		},
		Action: start,
	}
	return app
}

var (
	flagClusterID      = flags.ClusterID()
	flagTopicID        = flags.TopicID()
	flagLogStreamID    = flags.LogStreamID()
	flagMRAddrs        = flags.FlagDesc{Name: "address"}
	flagMsgSize        = flags.FlagDesc{Name: "msg-size"}
	flagBatchSize      = flags.FlagDesc{Name: "batch-size"}
	flagConcurrency    = flags.FlagDesc{Name: "concurrency"}
	flagDuration       = flags.FlagDesc{Name: "duration"}
	flagReportInterval = flags.FlagDesc{Name: "report-interval"}
)

func start(c *cli.Context) error {
	if c.NArg() > 0 {
		return fmt.Errorf("unexpected args: %v", c.Args().Slice())
	}

	clusterID, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}

	topicID, err := types.ParseTopicID(c.String(flagTopicID.Name))
	if err != nil {
		return err
	}

	logStreamID, err := types.ParseLogStreamID(c.String(flagLogStreamID.Name))
	if err != nil {
		return err
	}

	msgSize := c.Int(flagMsgSize.Name)
	if msgSize < 0 {
		return fmt.Errorf("stress: invalid msg size %d", msgSize)
	}

	batchSize := c.Int(flagBatchSize.Name)
	if batchSize <= 0 {
		return fmt.Errorf("stress: invalid batch size %d", batchSize)
	}

	concurrency := c.Int(flagConcurrency.Name)
	if concurrency < 1 {
		return fmt.Errorf("stress: invalid concurrency %d", concurrency)
	}

	duration := c.Duration(flagDuration.Name)

	reportInterval := c.Duration(flagReportInterval.Name)

	return benchmark.Append(
		benchmark.WithClusterID(clusterID),
		benchmark.WithTopicID(topicID),
		benchmark.WithLogStreamID(logStreamID),
		benchmark.WithMetadataRepository(c.StringSlice(flagMRAddrs.Name)),
		benchmark.WithMessageSize(msgSize),
		benchmark.WithBatchSize(batchSize),
		benchmark.WithConcurrency(concurrency),
		benchmark.WithDuration(duration),
		benchmark.WithReportInterval(reportInterval),
	)
}
