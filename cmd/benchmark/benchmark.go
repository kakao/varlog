package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark"
	"github.com/kakao/varlog/pkg/types"
)

var (
	flagClusterID = &cli.StringFlag{
		Name:  "cluster",
		Usage: "Cluster ID",
		Value: benchmark.DefaultClusterID.String(),
	}
	flagTarget = &cli.StringSliceFlag{
		Name:     "target",
		Required: true,
		Usage:    "The target of the benchmark load formatted by \"topic1:logstream1,topic2:logstream2,...<topic_id:logstream_id>\"",
	}
	flagMRAddrs = &cli.StringSliceFlag{
		Name:     "address",
		Required: true,
	}
	flagMsgSize = &cli.UintSliceFlag{
		Name:    "message-size",
		Aliases: []string{"msg-size"},
		Value:   cli.NewUintSlice(benchmark.DefaultMessageSize),
		Usage:   "Message sizes for each load target",
	}
	flagBatchSize = &cli.UintSliceFlag{
		Name:  "batch-size",
		Value: cli.NewUintSlice(benchmark.DefaultBatchSize),
		Usage: "Batch sizes for each load target",
	}
	flagAppenders = &cli.UintSliceFlag{
		Name:    "appenders",
		Aliases: []string{"appenders-count"},
		Value:   cli.NewUintSlice(benchmark.DefaultConcurrency),
		Usage:   "The number of appenders for each load target",
	}
	flagSubscribers = &cli.UintSliceFlag{
		Name:    "subscribers",
		Aliases: []string{"subscribers-count"},
		Value:   cli.NewUintSlice(benchmark.DefaultConcurrency),
		Usage:   "The number of subscribers for each load target",
	}
	flagDuration = &cli.DurationFlag{
		Name:  "duration",
		Value: benchmark.DefaultDuration,
	}
	flagReportInterval = &cli.DurationFlag{
		Name:  "report-interval",
		Value: benchmark.DefaultReportInterval,
	}
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
			flagClusterID,
			flagTarget,
			flagMRAddrs,
			flagMsgSize,
			flagBatchSize,
			flagAppenders,
			flagSubscribers,
			flagDuration,
			flagReportInterval,
		},
		Action: start,
	}
	return app
}

func start(c *cli.Context) error {
	if c.NArg() > 0 {
		return fmt.Errorf("unexpected args: %v", c.Args().Slice())
	}

	clusterID, err := types.ParseClusterID(c.String(flagClusterID.Name))
	if err != nil {
		return err
	}

	targets := make([]benchmark.Target, len(c.StringSlice(flagTarget.Name)))
	for idx, str := range c.StringSlice(flagTarget.Name) {
		toks := strings.Split(str, ":")
		if len(toks) != 2 {
			return fmt.Errorf("malformed target %s", str)
		}
		var target benchmark.Target
		target.TopicID, err = types.ParseTopicID(toks[0])
		if err != nil {
			return fmt.Errorf("malformed target %s: invalid topic %s", str, toks[0])
		}
		if toks[1] != "*" {
			target.LogStreamID, err = types.ParseLogStreamID(toks[1])
			if err != nil {
				return fmt.Errorf("malformed target %s: invalid log stream %s", str, toks[1])
			}
		}
		targets[idx] = target
	}

	targets = setSizes(targets, c.UintSlice(flagMsgSize.Name), func(idx int, size uint) {
		targets[idx].MessageSize = size
	})

	targets = setSizes(targets, c.UintSlice(flagBatchSize.Name), func(idx int, size uint) {
		targets[idx].BatchSize = size
	})

	targets = setSizes(targets, c.UintSlice(flagAppenders.Name), func(idx int, size uint) {
		targets[idx].AppendersCount = size
	})
	targets = setSizes(targets, c.UintSlice(flagSubscribers.Name), func(idx int, size uint) {
		targets[idx].SubscribersCount = size
	})

	duration := c.Duration(flagDuration.Name)

	reportInterval := c.Duration(flagReportInterval.Name)

	bm, err := benchmark.New(
		benchmark.WithClusterID(clusterID),
		benchmark.WithTargets(targets...),
		benchmark.WithMetadataRepository(c.StringSlice(flagMRAddrs.Name)),
		benchmark.WithDuration(duration),
		benchmark.WithReportInterval(reportInterval),
	)
	if err != nil {
		return err
	}
	defer func() {
		_ = bm.Close()
	}()
	return bm.Run()
}

func setSizes(targets []benchmark.Target, sizes []uint, setter func(idx int, size uint)) []benchmark.Target {
	for idx := range targets {
		var size uint
		if idx < len(sizes) {
			size = sizes[idx]
		} else {
			size = sizes[len(sizes)-1]
		}
		setter(idx, size)
	}
	return targets
}
