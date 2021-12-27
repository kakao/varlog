package main

import (
	"os"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"

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
		return -1
	}
	return 0
}

func newApp() *cli.App {
	app := &cli.App{
		Name:  "varlogcli",
		Usage: "Varlog client",
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

func newAppend() *cli.Command {
	return &cli.Command{
		Name:   cmdAppend,
		Action: commandAction,
		Flags:  commonFlags(),
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
	clusterID, err = types.ParseClusterID(c.String(flags.ClusterID().Name))
	if err != nil {
		return err
	}
	topicID, err = types.ParseTopicID(c.String(flags.TopicID().Name))
	if err != nil {
		return err
	}
	if c.IsSet(flags.LogStreamID().Name) {
		logStreamID, err = types.ParseLogStreamID(c.String(flags.LogStreamID().Name))
		if err != nil {
			return err
		}
	}

	switch c.Command.Name {
	case cmdAppend:
		if c.IsSet(flags.LogStreamID().Name) {
			return varlogcli.AppendTo(mrAddrs, clusterID, topicID, logStreamID)
		}
		return varlogcli.Append(mrAddrs, clusterID, topicID)
	case cmdSubscribe:
		if c.IsSet(flags.LogStreamID().Name) {
			return varlogcli.SubscribeTo(mrAddrs, clusterID, topicID, logStreamID)
		}
		return varlogcli.Subscribe(mrAddrs, clusterID, topicID)
	}
	return errors.Errorf("unexpected command: %s", c.Command.Name)
}

func commonFlags() []cli.Flag {
	return []cli.Flag{
		flags.MetadataRepositoryAddress().StringSliceFlag(true, nil),
		flags.ClusterID().StringFlag(false, types.ClusterID(1).String()),
		flags.TopicID().StringFlag(true, ""),
		flags.LogStreamID().StringFlag(false, ""),
	}
}
