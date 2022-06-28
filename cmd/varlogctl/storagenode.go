package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.daumkakao.com/varlog/varlog/internal/varlogctl"
	"github.daumkakao.com/varlog/varlog/internal/varlogctl/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

func newStorageNodeCommand() *cli.Command {
	const (
		cmdDescribe            = "get"
		cmdAdd                 = "add"
		cmdRemove              = "remove"
		cmdUnregisterLogStream = "unregister-log-stream"
	)
	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return fmt.Errorf("storage node command: unexpected args: %v", c.Args().Slice())
		}

		addr := c.String(flagSNAddr.name)
		var snid types.StorageNodeID
		if c.IsSet(flagStorageNodeID.name) {
			var err error
			snid, err = types.ParseStorageNodeID(c.String(flagStorageNodeID.name))
			if err != nil {
				return fmt.Errorf("storage node command: %w", err)
			}
		}

		var f varlogctl.ExecuteFunc
		switch c.Command.Name {
		case cmdDescribe:
			if c.IsSet(flagStorageNodeID.name) {
				f = storagenode.Describe(snid)
			} else {
				f = storagenode.Describe()
			}
		case cmdAdd:
			f = storagenode.Add(addr, snid)
		case cmdRemove:
			f = storagenode.Remove(addr, snid)

		case cmdUnregisterLogStream:
			panic("not implemented")
		default:
			return fmt.Errorf("storage node command: unknown subcommand: %s", c.Command.Name)
		}
		return execute(c, f)
	}
	return &cli.Command{
		Name:    "storagenode",
		Aliases: []string{"sn"},
		Subcommands: []*cli.Command{
			{
				Name:    cmdDescribe,
				Aliases: []string{"describe"},
				Action:  action,
				Flags: commonFlags(
					flagStorageNodeID.StringFlag(false, ""),
				),
			},
			{
				Name:   cmdAdd, // or register
				Action: action,
				Flags: commonFlags(
					flagSNAddr.StringFlag(true, ""),
					flagStorageNodeID.StringFlag(true, ""),
				),
			},
			{
				Name:   cmdRemove, // or unregister
				Action: action,
				Flags: commonFlags(
					flagSNAddr.StringFlag(true, ""),
					flagStorageNodeID.StringFlag(true, ""),
				),
			},
		},
	}
}
