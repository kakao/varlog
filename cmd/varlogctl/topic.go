package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/topic"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func newTopicCommand() *cli.Command {
	const (
		cmdDescribe = "get"
		cmdAdd      = "add"
		cmdRemove   = "remove"
	)

	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return fmt.Errorf("topic command: unexpected args: %v", c.Args().Slice())
		}

		var tpid types.TopicID
		if c.IsSet(flagTopicID.name) {
			var err error
			tpid, err = types.ParseTopicID(c.String(flagTopicID.name))
			if err != nil {
				return fmt.Errorf("topic command: %w", err)
			}
		}

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdDescribe:
			if c.IsSet(flagTopicID.name) {
				f = topic.Describe(tpid)
			} else {
				f = topic.Describe()
			}
		case cmdAdd:
			f = topic.Add()
		case cmdRemove:
			f = topic.Remove(tpid)
		default:
			return fmt.Errorf("topic command: unknown command: %s", c.Command.Name)
		}
		return execute(c, f)
	}

	return &cli.Command{
		Name: "topic",
		Subcommands: []*cli.Command{
			{
				Name:    cmdDescribe,
				Aliases: []string{"describe"},
				Usage:   "describe a topic",
				Action:  action,
				Flags: commonFlags(
					flagTopicID.StringFlag(false, ""),
				),
			},
			{
				Name:   cmdAdd,
				Usage:  "add a new topic",
				Action: action,
				Flags:  commonFlags(),
			},
			{
				Name:   cmdRemove,
				Usage:  "remove a topic",
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
				),
			},
		},
	}
}
