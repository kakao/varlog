package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/buildinfo"
	"github.com/kakao/varlog/internal/flags"
	"github.com/kakao/varlog/internal/varlogcli"
	"github.com/kakao/varlog/pkg/types"
)

func main() {
	os.Exit(run())
}

func run() int {
	app := newApp()
	if err := app.Run(os.Args); err != nil {
		log.Printf("error: %v", err)
		return -1
	}
	return 0
}

func newApp() *cli.App {
	buildInfo := buildinfo.ReadVersionInfo()
	cli.VersionPrinter = func(*cli.Context) {
		fmt.Println(buildInfo.String())
	}
	app := &cli.App{
		Name:    "varlogcli",
		Usage:   "Varlog client",
		Version: buildInfo.Version,
		Commands: []*cli.Command{
			newAppend(),
			newSubscribe(),
		},
	}
	return app
}

const (
	cmdAppend    = "append"
	cmdSubscribe = "subscribe"
)

var (
	flagBatchSize = flags.FlagDesc{Name: "batch-size"}
	flagTopicID   = func() *cli.IntFlag {
		f := flags.GetTopicIDFlag()
		f.Required = true
		return f
	}()
	flagLogStreamID = flags.GetLogStreamIDFlag()
)

func newAppend() *cli.Command {
	return &cli.Command{
		Name:   cmdAppend,
		Action: commandAction,
		Flags: append(
			commonFlags(),
			flagBatchSize.IntFlag(false, 1),
		),
	}
}

func newSubscribe() *cli.Command {
	return &cli.Command{
		Name:   cmdSubscribe,
		Action: commandAction,
		Flags:  commonFlags(),
	}
}

func commandAction(c *cli.Context) error {
	if c.NArg() > 0 {
		return errors.Errorf("unexpected args: %v", c.Args().Slice())
	}

	var (
		err         error
		mrAddrs     []string
		clusterID   types.ClusterID
		topicID     types.TopicID
		logStreamID types.LogStreamID
	)
	mrAddrs = c.StringSlice(flags.MetadataRepositoryAddress().Name)
	clusterID, err = types.ParseClusterID(c.String(flags.ClusterID.Name))
	if err != nil {
		return err
	}
	topicID = types.TopicID(flagTopicID.Get(c))
	if c.IsSet(flagLogStreamID.Name) {
		logStreamID = types.LogStreamID(flagLogStreamID.Get(c))
	}

	switch c.Command.Name {
	case cmdAppend:
		batchSize := c.Int(flagBatchSize.Name)
		if batchSize < 1 {
			return errors.New("invalid batch size")
		}

		if c.IsSet(flagLogStreamID.Name) {
			return varlogcli.AppendTo(mrAddrs, clusterID, topicID, logStreamID, batchSize)
		}
		return varlogcli.Append(mrAddrs, clusterID, topicID, batchSize)
	case cmdSubscribe:
		if c.IsSet(flagLogStreamID.Name) {
			return varlogcli.SubscribeTo(mrAddrs, clusterID, topicID, logStreamID)
		}
		return varlogcli.Subscribe(mrAddrs, clusterID, topicID)
	}
	return errors.Errorf("unexpected command: %s", c.Command.Name)
}

func commonFlags() []cli.Flag {
	return []cli.Flag{
		flags.ClusterID,
		flags.MetadataRepositoryAddress().StringSliceFlag(true, nil),
		flagTopicID,
		flagLogStreamID,
	}
}
