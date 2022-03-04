package main

import (
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/flags"
	"github.daumkakao.com/varlog/varlog/internal/stress"
	"github.daumkakao.com/varlog/varlog/pkg/types"
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
		Name:  "stress",
		Usage: "stress client",
		Flags: []cli.Flag{
			flagClusterID.StringFlag(false, types.ClusterID(1).String()),
			flagTopicID.StringFlag(false, types.TopicID(1).String()),
			flagLogStreamID.StringFlag(false, types.LogStreamID(1).String()),
			flagMRAddrs.StringSliceFlag(true, nil),
			flagMsgSize.IntFlag(false, 1024),
			flagBatchSize.IntFlag(false, 128),
			flagConcurrency.IntFlag(false, 10),
			flagDuration.DurationFlag(false, 10*time.Minute),
			flagReportInterval.DurationFlag(false, 3*time.Second),
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

	return stress.Append(stress.Config{
		MRAddrs:        c.StringSlice(flagMRAddrs.Name),
		ClusterID:      clusterID,
		TopicID:        topicID,
		LogStreamID:    logStreamID,
		MessageSize:    msgSize,
		BatchSize:      batchSize,
		Concurrency:    concurrency,
		Duration:       duration,
		ReportInterval: reportInterval,
	})
}
