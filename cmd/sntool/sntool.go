package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type sntool struct {
	snAddr    string
	clusterID types.ClusterID
	timeout   time.Duration

	cmd *cobra.Command
}

func main() {
	snt := &sntool{}
	snt.initCommand()
	if err := snt.cmd.Execute(); err != nil {
		fmt.Println(err)
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
	snt.cmd = &cobra.Command{
		Use:  "sntool",
		Long: "sntool",
	}
	getCmd := &cobra.Command{
		Use:  "get",
		Args: cobra.NoArgs,
	}
	getCmd.Flags().DurationVar(&snt.timeout, "timeout", 10*time.Second, "timeout")
	getCmd.Flags().StringVar(&snt.snAddr, "storage-node-address", "", "storage node address")
	getCmd.MarkFlagRequired("storage-node-address")
	getCmd.Flags().Uint32Var((*uint32)(&snt.clusterID), "cluster-id", 1, "cluster id")
	getCmd.Run = func(cmd *cobra.Command, args []string) {
		str, err := snt.get()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(str)
	}
	snt.cmd.AddCommand(getCmd)
}
