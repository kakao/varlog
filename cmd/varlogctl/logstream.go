package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/internal/varlogctl/logstream"
	"github.com/kakao/varlog/pkg/types"
)

func newLogStreamCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdSeal     = "seal"
		cmdUnseal   = "unseal"
		cmdSync     = "sync"
		cmdDescribe = "get"
		cmdRecover  = "recover"
	)

	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return fmt.Errorf("log stream command: unexpected args: %v", c.Args().Slice())
		}

		var (
			topicID     types.TopicID
			logStreamID types.LogStreamID
			err         error
		)

		if c.IsSet(flagTopicID.name) {
			topicID, err = types.ParseTopicID(c.String(flagTopicID.name))
			if err != nil {
				return fmt.Errorf("log stream command: %w", err)
			}
		}
		if c.IsSet(flagLogStreamID.name) {
			logStreamID, err = types.ParseLogStreamID(c.String(flagLogStreamID.name))
			if err != nil {
				return fmt.Errorf("log stream command: %w", err)
			}
		}

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdAdd:
			f = logstream.Add(topicID)
		case cmdSeal:
			f = logstream.Seal(topicID, logStreamID)
		case cmdUnseal:
			f = logstream.Unseal(topicID, logStreamID)
		case cmdSync:
			src, err := types.ParseStorageNodeID(c.String(flagSyncSrc.name))
			if err != nil {
				return fmt.Errorf("log stream command: %w", err)
			}
			dst, err := types.ParseStorageNodeID(c.String(flagSyncDst.name))
			if err != nil {
				return fmt.Errorf("log stream command: %w", err)
			}
			f = logstream.Sync(topicID, logStreamID, src, dst)
		case cmdDescribe:
			if c.IsSet(flagLogStreamID.name) {
				f = logstream.Describe(topicID, logStreamID)
			} else {
				f = logstream.Describe(topicID)
			}
		case cmdRecover:
			panic("not implemented")
		}
		return execute(c, f)
	}
	return &cli.Command{
		Name:    "logstream",
		Aliases: []string{"ls"},
		Subcommands: []*cli.Command{
			{
				Name:   cmdAdd,
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
				),
			},
			{
				Name:   cmdSeal,
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
				),
			},
			{
				Name:   cmdUnseal,
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
				),
			},
			{
				Name:   cmdSync,
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
					flagSyncSrc.StringFlag(true, ""),
					flagSyncDst.StringFlag(true, ""),
				),
			},
			{
				Name:    cmdDescribe,
				Aliases: []string{"describe"},
				Action:  action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(false, ""),
				),
			},
			{
				Name:   cmdRecover,
				Action: action,
				Flags: commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(false, ""),
					// TODO: define necessary flags
				),
			},
		},
	}
}
