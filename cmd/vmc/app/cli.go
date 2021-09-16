package app

import (
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

func (app *VMCApp) initCLI() {
	app.app = cli.NewApp()
	app.app.Name = "vmc"
	app.app.Description = "vmc is client for varlog manager server"
	app.app.Version = "v0.0.1"

	// cli.VersionFlag = &cli.BoolFlag{Name: "version"}
	app.app.Flags = append(app.app.Flags,
		&cli.StringFlag{
			Name:        "vms-address",
			EnvVars:     []string{"VMS_ADDRESS"},
			Value:       DefaultVMSAddress,
			Destination: &app.options.VMSAddress,
		},
		&cli.DurationFlag{
			Name:        "rpc-timeout",
			EnvVars:     []string{"RPC_TIMEOUT"},
			Value:       DefaultTimeout,
			Destination: &app.options.Timeout,
		},
		&cli.StringFlag{
			Name:        "output",
			EnvVars:     []string{"OUTPUT"},
			Value:       DefaultPrinter,
			Destination: &app.options.Output,
		},
		&cli.BoolFlag{
			Name:        "verbose",
			EnvVars:     []string{"VERBOSE"},
			Value:       DefaultVerbose,
			Destination: &app.options.Verbose,
		},
	)

	app.app.Commands = append(app.app.Commands,
		app.initAddCmd(),
		app.initRmCmd(),
		app.initUpdateCmd(),
		app.initSealCmd(),
		app.initUnsealCmd(),
		app.initSyncCmd(),
		app.initMetaCmd(),
		app.initMRMgmtCmd(),
	)
}

func (app *VMCApp) initAddCmd() *cli.Command {
	// vmc add
	cmd := &cli.Command{
		Name: "add",
	}

	// vmc add storagenode
	snCmd := newSNCmd()
	snCmd.Flags = append(snCmd.Flags, &cli.StringFlag{
		Name:    "storage-node-address",
		Usage:   "storage node address",
		EnvVars: []string{"STORAGE_NODE_ADDRESS"},
	})
	snCmd.Action = func(c *cli.Context) error {
		snAddr := c.String("storage-node-address")
		app.addStorageNode(snAddr)
		return nil
	}

	// vmc add topic
	tpCmd := newTopicCmd()
	tpCmd.Flags = append(tpCmd.Flags, &cli.StringFlag{})
	tpCmd.Action = func(c *cli.Context) error {
		app.addTopic()
		return nil
	}

	// vmc add logstream
	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags, &cli.StringFlag{
		Name:     "topic-id",
		Usage:    "topic identifier",
		EnvVars:  []string{"TOPIC_ID"},
		Required: true,
	})
	lsCmd.Action = func(c *cli.Context) error {
		topicID, err := types.ParseTopicID(c.String("topic-id"))
		if err != nil {
			return err
		}
		app.addLogStream(topicID)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, snCmd, tpCmd, lsCmd)
	return cmd
}

func (app *VMCApp) initRmCmd() *cli.Command {
	// vmc remove
	cmd := &cli.Command{
		Name:    "remove",
		Aliases: []string{"rm"},
	}

	// vmc remove storagenode
	snCmd := newSNCmd()
	snCmd.Flags = append(snCmd.Flags, &cli.StringFlag{
		Name:     "storage-node-id",
		Usage:    "storage node identifier",
		EnvVars:  []string{"STORAGE_NODE_ID"},
		Required: true,
	})
	snCmd.Action = func(c *cli.Context) error {
		snID, err := types.ParseStorageNodeID(c.String("storage-node-id"))
		if err != nil {
			return err
		}
		app.removeStorageNode(snID)
		return nil
	}

	// vmc remove logstream
	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags,
		&cli.StringFlag{
			Name:     "topic-id",
			Usage:    "topic identifier",
			EnvVars:  []string{"TOPIC_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "log-stream-id",
			Usage:    "log stream identifier",
			EnvVars:  []string{"LOG_STREAM_ID"},
			Required: true,
		},
	)
	lsCmd.Action = func(c *cli.Context) error {
		topicID, err := types.ParseTopicID(c.String("topic-id"))
		if err != nil {
			return err
		}
		lsID, err := types.ParseLogStreamID(c.String("log-stream-id"))
		if err != nil {
			return err
		}
		app.removeLogStream(topicID, lsID)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, snCmd, lsCmd)
	return cmd
}

func (app *VMCApp) initUpdateCmd() *cli.Command {
	// vmc update
	cmd := &cli.Command{
		Name:    "update",
		Aliases: []string{"mv"},
	}

	// vmc update logstream
	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags,
		&cli.StringFlag{
			Name:     "log-stream-id",
			Usage:    "log stream identifier",
			EnvVars:  []string{"LOG_STREAM_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:    "pop-storage-node-id",
			Usage:   "storage node identifier",
			EnvVars: []string{"POP_STORAGE_NODE_ID"},
		},
		&cli.StringFlag{
			Name:    "push-storage-node-id",
			Usage:   "storage node identifier",
			EnvVars: []string{"PUSH_STORAGE_NODE_ID"},
		},
		&cli.StringFlag{
			Name:    "push-storage-path",
			Usage:   "storage node path",
			EnvVars: []string{"PUSH_STORAGE_PATH"},
		},
	)
	lsCmd.Action = func(c *cli.Context) error {
		var (
			popReplica  *varlogpb.ReplicaDescriptor
			pushReplica *varlogpb.ReplicaDescriptor
		)

		lsID, err := types.ParseLogStreamID(c.String("log-stream-id"))
		if err != nil {
			return err
		}
		if c.IsSet("pop-storage-node-id") {
			popSNID, err := types.ParseStorageNodeID(c.String("pop-storage-node-id"))
			if err != nil {
				return err
			}
			popReplica = &varlogpb.ReplicaDescriptor{
				StorageNodeID: popSNID,
			}
		}

		if c.IsSet("push-storage-node-id") && c.IsSet("push-storage-path") {
			pushSNID, err := types.ParseStorageNodeID(c.String("push-storage-node-id"))
			if err != nil {
				return err
			}
			pushPath := c.String("push-storage-path")
			pushReplica = &varlogpb.ReplicaDescriptor{
				StorageNodeID: pushSNID,
				Path:          pushPath,
			}
		}
		app.updateLogStream(lsID, popReplica, pushReplica)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, lsCmd)
	return cmd
}

func (app *VMCApp) initSealCmd() *cli.Command {
	// vmc seal lotstream
	cmd := &cli.Command{
		Name: "seal",
	}

	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags, &cli.StringFlag{
		Name:     "topic-id",
		Usage:    "topic identifier",
		EnvVars:  []string{"TOPIC_ID"},
		Required: true,
	})
	lsCmd.Flags = append(lsCmd.Flags, &cli.StringFlag{
		Name:     "log-stream-id",
		Usage:    "log stream identifier",
		EnvVars:  []string{"LOG_STREAM_ID"},
		Required: true,
	})
	lsCmd.Action = func(c *cli.Context) error {
		tpID, err := types.ParseTopicID(c.String("topic-id"))
		if err != nil {
			return err
		}
		lsID, err := types.ParseLogStreamID(c.String("log-stream-id"))
		if err != nil {
			return err
		}
		app.sealLogStream(tpID, lsID)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, lsCmd)
	return cmd
}

func (app *VMCApp) initUnsealCmd() *cli.Command {
	// vmc unseal lotstream
	cmd := &cli.Command{
		Name: "unseal",
	}

	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags, &cli.StringFlag{
		Name:     "topic-id",
		Usage:    "topic identifier",
		EnvVars:  []string{"TOPIC_ID"},
		Required: true,
	})
	lsCmd.Flags = append(lsCmd.Flags, &cli.StringFlag{
		Name:     "log-stream-id",
		Usage:    "log stream identifier",
		EnvVars:  []string{"LOG_STREAM_ID"},
		Required: true,
	})
	lsCmd.Action = func(c *cli.Context) error {
		tpID, err := types.ParseTopicID(c.String("topic-id"))
		if err != nil {
			return err
		}
		lsID, err := types.ParseLogStreamID(c.String("log-stream-id"))
		if err != nil {
			return err
		}
		app.unsealLogStream(tpID, lsID)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, lsCmd)

	return cmd
}

func (app *VMCApp) initSyncCmd() *cli.Command {
	// vmc sync lotstream
	cmd := &cli.Command{
		Name: "sync",
	}

	lsCmd := newLSCmd()
	lsCmd.Flags = append(lsCmd.Flags,
		&cli.StringFlag{
			Name:     "topic-id",
			Usage:    "topic identifier",
			EnvVars:  []string{"TOPIC_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "log-stream-id",
			Usage:    "log stream identifier",
			EnvVars:  []string{"LOG_STREAM_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "source-storage-node-id",
			Usage:    "source storage node identifier",
			EnvVars:  []string{"SOURCE_STORAGE_NODE_ID"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "destination-storage-node-id",
			Usage:    "destination storage node identifier",
			EnvVars:  []string{"DESTINATION_STORAGE_NODE_ID"},
			Required: true,
		},
	)
	lsCmd.Action = func(c *cli.Context) error {
		tpID, err := types.ParseTopicID(c.String("topic-id"))
		if err != nil {
			return err
		}
		lsID, err := types.ParseLogStreamID(c.String("log-stream-id"))
		if err != nil {
			return err
		}
		srcSNID, err := types.ParseStorageNodeID(c.String("source-storage-node-id"))
		if err != nil {
			return err
		}
		dstSNID, err := types.ParseStorageNodeID(c.String("destination-storage-node-id"))
		if err != nil {
			return err
		}
		app.syncLogStream(tpID, lsID, srcSNID, dstSNID)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, lsCmd)
	return cmd
}

func (app *VMCApp) initMetaCmd() *cli.Command {
	// vmc metadata
	cmd := &cli.Command{
		Name:    "metadata",
		Aliases: []string{"meta"},
	}

	// vmc metdata storagenode
	snCmd := newSNCmd()
	snCmd.Action = func(c *cli.Context) error {
		app.infoStoragenodes()
		return nil
	}

	// vmc metdata logstream
	lsCmd := newLSCmd()
	lsCmd.Action = func(c *cli.Context) error {
		panic("not implemented")
	}

	// vmc metdata metadatarepository
	mrCmd := newMRCmd()
	mrCmd.Action = func(c *cli.Context) error {
		panic("not implemented")
	}

	// vmc metadata cluster
	clusCmd := newClusterCmd()
	clusCmd.Action = func(c *cli.Context) error {
		panic("not implemented")
	}

	cmd.Subcommands = append(cmd.Subcommands, snCmd, lsCmd, mrCmd, clusCmd)

	return cmd
}

func (app *VMCApp) initMRMgmtCmd() *cli.Command {
	cmd := &cli.Command{
		Name:    "metadatarepository",
		Aliases: []string{"mr"},
	}

	infoCmd := &cli.Command{
		Name: "info",
	}
	infoCmd.Action = func(c *cli.Context) error {
		app.infoMRMembers()
		return nil
	}

	addCmd := &cli.Command{
		Name: "add",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "raft-url",
				Usage:    "metadata repository raft url",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "rpc-addr",
				Usage:    "metadata repository rpc addr",
				Required: true,
			},
		},
	}
	addCmd.Action = func(c *cli.Context) error {
		raftURL := c.String("raft-url")
		rpcAddr := c.String("rpc-addr")
		app.addMRPeer(raftURL, rpcAddr)
		return nil
	}

	rmCmd := &cli.Command{
		Name: "remove",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "raft-url",
				Usage:    "metadata repository raft url",
				Required: true,
			},
		},
	}
	rmCmd.Action = func(c *cli.Context) error {
		raftURL := c.String("raft-url")
		app.removeMRPeer(raftURL)
		return nil
	}

	cmd.Subcommands = append(cmd.Subcommands, infoCmd, addCmd, rmCmd)
	return cmd
}

func addStorageNodeIDFlag(flags []cli.Flag, required bool) []cli.Flag {
	return append(flags, &cli.StringFlag{
		Name:     "storage-node-id",
		Usage:    "storage node identifier",
		EnvVars:  []string{"STORAGE_NODE_ID"},
		Required: required,
	})
}

func addLogStreamIDFlag(flags []cli.Flag, required bool) []cli.Flag {
	return append(flags, &cli.StringFlag{
		Name:     "log-stream-id",
		Usage:    "log stream identifier",
		EnvVars:  []string{"LOG_STREAM_ID"},
		Required: required,
	})
}

func newSNCmd() *cli.Command {
	return &cli.Command{
		Name:    "storagenode",
		Aliases: []string{"sn"},
	}
}

func newTopicCmd() *cli.Command {
	return &cli.Command{
		Name:    "topic",
		Aliases: []string{"t"},
	}
}

func newLSCmd() *cli.Command {
	return &cli.Command{
		Name:    "logstream",
		Aliases: []string{"ls"},
	}
}

func newMRCmd() *cli.Command {
	return &cli.Command{
		Name:    "metadatarepository",
		Aliases: []string{"mr"},
	}
}

func newClusterCmd() *cli.Command {
	return &cli.Command{
		Name: "cluster",
	}
}
