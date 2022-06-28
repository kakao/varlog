package main

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/internal/varlogctl"
	"github.com/kakao/varlog/internal/varlogctl/metarepos"
)

func newMetadataRepositoryCommand() *cli.Command {
	const (
		cmdAdd      = "add"
		cmdRemove   = "remove"
		cmdDescribe = "get"
	)
	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return fmt.Errorf("metadata repository command: unexpected args: %v", c.Args().Slice())
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
			return fmt.Errorf("metadata repository command: unknown command: %s", c.Command.Name)
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
				Flags: commonFlags(
					flagMRRaftURL.StringFlag(true, ""),
					flagMRRPCAddr.StringFlag(true, ""),
				),
			},
			{
				Name:   cmdRemove,
				Usage:  "remove a metadata repository",
				Action: action,
				Flags: commonFlags(
					flagMRRaftURL.StringFlag(true, ""),
				),
			},
			{
				Name:    cmdDescribe,
				Aliases: []string{"describe"},
				Action:  action,
				Flags: commonFlags(
					flagMRRaftURL.StringFlag(false, ""),
				),
			},
		},
	}
}
