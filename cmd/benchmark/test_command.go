package main

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/benchmark"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/pkg/types"
)

var (
	flagClusterID = flags.ClusterID

	flagTarget = &cli.StringSliceFlag{
		Name:     "target",
		Category: "Common: ",
		Required: true,
		Usage:    "The target of the benchmark load formatted by \"topic1:logstream1,topic2:logstream2,...<topic_id:logstream_id>\"",
	}
	flagMRAddrs = &cli.StringSliceFlag{
		Name:     "address",
		Category: "Common: ",
		Required: true,
	}
	flagDuration = &cli.DurationFlag{
		Name:     "duration",
		Category: "Common: ",
		Value:    benchmark.DefaultDuration,
	}
	flagReportInterval = &cli.DurationFlag{
		Name:     "report-interval",
		Category: "Common: ",
		Value:    benchmark.DefaultReportInterval,
	}
	flagPrintJSON = &cli.BoolFlag{
		Name:     "print-json",
		Category: "Common: ",
		Usage:    "Print json output if it is set",
	}
	flagSingleConnPerTarget = &cli.BoolFlag{
		Name:     "single-conn-per-target",
		Category: "Common: ",
		Usage:    "Use single connection shared by appenders in a target. Each target uses different connection.",
	}

	flagAppenders = &cli.UintSliceFlag{
		Name:     "appenders",
		Category: "Append: ",
		Aliases:  []string{"appenders-count"},
		Value:    cli.NewUintSlice(benchmark.DefaultConcurrency),
		Usage:    "The number of appenders for each load target",
	}
	flagMsgSize = &cli.UintSliceFlag{
		Name:     "message-size",
		Category: "Append: ",
		Aliases:  []string{"msg-size"},
		Value:    cli.NewUintSlice(benchmark.DefaultMessageSize),
		Usage:    "Message sizes for each load target",
	}
	flagBatchSize = &cli.UintSliceFlag{
		Name:     "batch-size",
		Category: "Append: ",
		Value:    cli.NewUintSlice(benchmark.DefaultBatchSize),
		Usage:    "Batch sizes for each load target",
	}
	flagPipelineSize = &cli.IntFlag{
		Name:     "pipeline-size",
		Category: "Append: ",
		Usage:    "Pipeline size, no pipelined requests if zero. Not support per-target pipeline size yet.",
		Value:    0,
	}

	flagSubscribers = &cli.UintSliceFlag{
		Name:     "subscribers",
		Category: "Subscribe: ",
		Aliases:  []string{"subscribers-count"},
		Value:    cli.NewUintSlice(benchmark.DefaultConcurrency),
		Usage:    "The number of subscribers for each load target",
	}
	flagSubscribeSize = &cli.IntFlag{
		Name:     "subscribe-size",
		Category: "Subscribe: ",
		Usage:    "The number of messages to subscribe at once",
		Value:    1000,
	}
)

func newCommandTest() *cli.Command {
	return &cli.Command{
		Name:  "test",
		Usage: "run benchmark test",
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
			flagPrintJSON,
			flagPipelineSize,
			flagSingleConnPerTarget,
			flagSubscribeSize,
		},
		Action: runCommandTest,
	}
}

func runCommandTest(c *cli.Context) error {
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
		target.PipelineSize = c.Int(flagPipelineSize.Name)
		target.SubscribeSize = c.Int(flagSubscribeSize.Name)
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

	var enc benchmark.ReportEncoder
	if c.Bool(flagPrintJSON.Name) {
		enc = benchmark.JsonEncoder{}
	} else {
		enc = benchmark.StringEncoder{}
	}

	opts := []benchmark.Option{
		benchmark.WithClusterID(clusterID),
		benchmark.WithTargets(targets...),
		benchmark.WithMetadataRepository(c.StringSlice(flagMRAddrs.Name)),
		benchmark.WithDuration(duration),
		benchmark.WithReportInterval(reportInterval),
		benchmark.WithReportEncoder(enc),
	}
	if c.Bool(flagSingleConnPerTarget.Name) {
		opts = append(opts, benchmark.WithSingleConnPerTarget())
	}
	bm, err := benchmark.New(opts...)
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
