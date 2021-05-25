package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
)

type sntool struct {
	snAddr    string
	clusterID types.ClusterID
	timeout   time.Duration

	cmd *cli.App
}

func main() {
	snt := &sntool{}
	snt.initCommand()
	if err := snt.cmd.Run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func (snt *sntool) get() (string, error) {
	ctx := context.Background()
	snmcl, err := snc.NewManagementClient(ctx, snt.clusterID, snt.snAddr, zap.NewNop())
	if err != nil {
		return "", errors.New("sntool: could not connect storage node")
	}
	defer snmcl.Close()
	snmeta, err := snmcl.GetMetadata(ctx)
	if err != nil {
		return "", errors.New("sntool: could not get metadata")
	}

	marshaler := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
	}
	str, err := marshaler.MarshalToString(snmeta)
	if err != nil {
		return "", fmt.Errorf("sntool: %w", err)
	}
	return str, nil
}

func (snt *sntool) initCommand() {
	snt.cmd = cli.NewApp()
	snt.cmd.Name = "sntool"
	snt.cmd.Version = "v0.0.1"
	snt.cmd.Commands = append(snt.cmd.Commands, &cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:        "timeout",
				Value:       10 * time.Second,
				Destination: &snt.timeout,
			},
			&cli.StringFlag{
				Name:        "storage-node-address",
				Required:    true,
				Destination: &snt.snAddr,
			},
			&cli.StringFlag{
				Name:  "cluster-id",
				Value: "1",
			},
		},
	})
	snt.cmd.Action = func(c *cli.Context) error {
		cid, err := types.ParseClusterID(c.String("cluster-id"))
		if err != nil {
			return err
		}
		snt.clusterID = cid
		str, err := snt.get()
		if err != nil {
			return err
		}
		fmt.Println(str)
		return nil
	}
}
