package main

import (
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/internal/varlogctl/logstream"
	"github.com/kakao/varlog/internal/varlogctl/metarepos"
	"github.com/kakao/varlog/internal/varlogctl/storagenode"
	"github.com/kakao/varlog/internal/varlogctl/topic"
	"github.com/kakao/varlog/pkg/types"
)

const (
	appName = "varlogctl"
	version = "0.0.1"

	defaultTimeout = time.Second * 5
)

func commonFlags(flags ...cli.Flag) []cli.Flag {
	return append([]cli.Flag{
		flagAdminAddress.StringFlag(true, ""),
		flagTimeout.DurationFlag(false, defaultTimeout),
		flagPrettyPrint.BoolFlag(),
		flagVerbose.BoolFlag(),
	}, flags...)
}

func newVarlogControllerApp() *cli.App {
	app := &cli.App{
		Name:     appName,
		Usage:    "controller application for varlog",
		Version:  version,
		Compiled: time.Now(),
		Commands: []*cli.Command{
			newTopicCommand(),
			newMetadataRepositoryCommand(),
			newStorageNodeCommand(),
			newLogStreamCommand(),
		},
	}
	return app
}

func newTopicCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdRemove   = "remove"
		cmdDescribe = "describe"
	)

	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return errors.Errorf("unexpected args: %v", c.Args().Slice())
		}

		var topicID types.TopicID
		if c.IsSet(flagTopicID.name) {
			var err error
			topicID, err = types.ParseTopicID(c.String(flagTopicID.name))
			if err != nil {
				return err
			}
		}

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdAdd:
			f = topic.Add()
		case cmdRemove:
			f = topic.Remove(topicID)
		case cmdDescribe:
			if c.IsSet(flagTopicID.name) {
				f = topic.Describe(topicID)
			} else {
				f = topic.Describe()
			}
		default:
			return errors.Errorf("unknown command: %s", c.Command.Name)
		}
		return execute(c, f)
	}

	return &cli.Command{
		Name: "topic",
		Subcommands: []*cli.Command{
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
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdDescribe,
				Usage:  "describe a topic",
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(false, ""),
				)),
			},
		},
	}
}

func newMetadataRepositoryCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdRemove   = "remove"
		cmdDescribe = "describe"
	)
	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return errors.Errorf("unexpected args: %v", c.Args().Slice())
		}

		raftURL, rpcAddr := c.String(flagMRRaftURL.name), c.String(flagMRRPCAddr.name)

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdAdd:
			f = metarepos.Add(raftURL, rpcAddr)
		case cmdRemove:
			f = metarepos.Remove(raftURL)
		case cmdDescribe:
			if c.IsSet(flagMRRaftURL.name) {
				f = metarepos.Describe(raftURL)
			} else {
				f = metarepos.Describe()
			}
		default:
			return errors.Errorf("unknown command: %s", c.Command.Name)
		}
		return execute(c, f)
	}
	return &cli.Command{
		Name:    "metadatarepository",
		Aliases: []string{"metarepos", "mr"},
		Subcommands: []*cli.Command{
			{
				Name:   cmdAdd,
				Usage:  "add a new metadata repository",
				Action: action,
				Flags: append(commonFlags(
					flagMRRaftURL.StringFlag(true, ""),
					flagMRRPCAddr.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdRemove,
				Usage:  "remove a metadata repository",
				Action: action,
				Flags: append(commonFlags(
					flagMRRaftURL.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdDescribe,
				Action: action,
				Flags: append(commonFlags(
					flagMRRaftURL.StringFlag(false, ""),
				)),
			},
		},
	}
}

func newStorageNodeCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdRemove   = "remove"
		cmdDescribe = "describe"
		// TODO: precise naming
		cmdUnregisterLogStream = "unregister-log-stream"
	)
	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return errors.Errorf("unexpected args: %v", c.Args().Slice())
		}

		addr := c.String(flagSNAddr.name)
		var storageNodeID types.StorageNodeID
		if c.IsSet(flagStorageNodeID.name) {
			var err error
			storageNodeID, err = types.ParseStorageNodeID(c.String(flagStorageNodeID.name))
			if err != nil {
				return err
			}
		}

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdAdd:
			f = storagenode.Add(addr, storageNodeID)
		case cmdRemove:
			f = storagenode.Remove(addr, storageNodeID)
		case cmdDescribe:
			if c.IsSet(flagStorageNodeID.name) {
				f = storagenode.Describe(storageNodeID)
			} else {
				f = storagenode.Describe()
			}
		case cmdUnregisterLogStream:
			panic("not implemented")
		default:
			return errors.Errorf("unknown command: %s", c.Command.Name)
		}
		return execute(c, f)
	}
	return &cli.Command{
		Name:    "storagenode",
		Aliases: []string{"sn"},
		Subcommands: []*cli.Command{
			{
				Name:   cmdAdd, // or register
				Action: action,
				Flags: append(commonFlags(
					flagSNAddr.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdRemove, // or unregister
				Action: action,
				Flags: append(commonFlags(
					flagSNAddr.StringFlag(true, ""),
					flagStorageNodeID.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdDescribe,
				Action: action,
				Flags: append(commonFlags(
					flagStorageNodeID.StringFlag(false, ""),
				)),
			},
		},
	}
}

func newLogStreamCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdSeal     = "seal"
		cmdUnseal   = "unseal"
		cmdSync     = "sync"
		cmdDescribe = "describe"
		cmdRecover  = "recover"
	)

	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return errors.Errorf("unexpected args: %v", c.Args().Slice())
		}

		var (
			topicID     types.TopicID
			logStreamID types.LogStreamID
			err         error
		)

		if c.IsSet(flagTopicID.name) {
			topicID, err = types.ParseTopicID(c.String(flagTopicID.name))
			if err != nil {
				return err
			}
		}
		if c.IsSet(flagLogStreamID.name) {
			logStreamID, err = types.ParseLogStreamID(c.String(flagLogStreamID.name))
			if err != nil {
				return err
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
				return err
			}
			dst, err := types.ParseStorageNodeID(c.String(flagSyncDst.name))
			if err != nil {
				return err
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
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdSeal,
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdUnseal,
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdSync,
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(true, ""),
					flagSyncSrc.StringFlag(true, ""),
					flagSyncDst.StringFlag(true, ""),
				)),
			},
			{
				Name:   cmdDescribe,
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(false, ""),
				)),
			},
			{
				Name:   cmdRecover,
				Action: action,
				Flags: append(commonFlags(
					flagTopicID.StringFlag(true, ""),
					flagLogStreamID.StringFlag(false, ""),
					// TODO: define necessary flags
				)),
			},
		},
	}
}
