package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/types"
)

const (
	appName = "mrtool"
	version = "0.0.1"

	flagClusterID = "cluster-id"
	flagAddress   = "address"
	flagTimeout   = "timeout"

	defaultClusterID = types.ClusterID(1)
	defaultTimeout   = time.Second
)

func run() int {
	const cmdDescribe = "describe"

	action := func(c *cli.Context) error {
		if c.NArg() > 0 {
			return errors.Errorf("unexpected args: %v", c.Args().Slice())
		}

		if c.Command.Name == cmdDescribe {
			return describe(c)
		}
		return errors.Errorf("unknown command: %s", c.Command.Name)
	}

	app := &cli.App{
		Name:    appName,
		Version: version,
		Commands: []*cli.Command{
			{
				Name:   cmdDescribe,
				Action: action,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  flagClusterID,
						Value: defaultClusterID.String(),
					},
					&cli.StringFlag{
						Name:     flagAddress,
						Required: true,
					},
					&cli.DurationFlag{
						Name:  flagTimeout,
						Value: defaultTimeout,
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return -1
	}
	return 0
}

func describe(c *cli.Context) error {
	clusterID, err := types.ParseClusterID(c.String(flagClusterID))
	if err != nil {
		return err
	}
	address := c.String(flagAddress)
	timeout := c.Duration(flagTimeout)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	mcl, err := mrc.NewMetadataRepositoryManagementClient(ctx, address)
	if err != nil {
		return err
	}

	rsp, err := mcl.GetClusterInfo(ctx, clusterID)
	if err != nil {
		return err
	}

	buf, err := json.Marshal(rsp.GetClusterInfo())
	if err != nil {
		return err
	}

	fmt.Println(string(buf))
	return nil
}

func main() {
	os.Exit(run())
}
