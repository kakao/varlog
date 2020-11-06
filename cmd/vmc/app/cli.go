package app

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func (app *VMCApp) initCLI() {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	app.rootCmd = &cobra.Command{
		Use:   "vmc",
		Short: "vmc is client for varlog manager server",
	}
	app.rootCmd.PersistentFlags().String("vms-address", DefaultVMSAddress, "vms address")
	app.rootCmd.PersistentFlags().Duration("rpc-timeout", DefaultTimeout, "rpc timeout")
	app.rootCmd.PersistentFlags().String("output", DefaultPrinter, "output printer")
	app.rootCmd.PersistentFlags().Bool("verbose", DefaultVerbose, "verbose")

	viper.BindEnv("vms-address")
	viper.BindEnv("rpc-timeout")
	viper.BindEnv("output")
	viper.BindEnv("verbose")
	viper.BindPFlags(app.rootCmd.PersistentFlags())

	app.rootCmd.AddCommand(
		app.initVersionCmd(),
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

func (app *VMCApp) initVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use: "version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("v0.0.1")
		},
	}
}

func (app *VMCApp) initAddCmd() *cobra.Command {
	// vmc add
	cmd := &cobra.Command{
		Use:     "add",
		Args:    cobra.NoArgs,
		Aliases: []string{},
	}

	var subCmd *cobra.Command

	// vmc add storagenode
	snAddr := ""
	subCmd = newSNCmd()
	subCmd.Flags().StringVar(&snAddr, "storage-node-address", "", "storage node address")
	subCmd.MarkFlagRequired("storage-node-address")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.addStorageNode(snAddr)
	}
	cmd.AddCommand(subCmd)

	// vmc add logstream
	subCmd = newLSCmd()
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.addLogStream()
	}
	cmd.AddCommand(subCmd)

	return cmd
}

func (app *VMCApp) initRmCmd() *cobra.Command {
	// vmc remove
	cmd := &cobra.Command{
		Use:     "remove",
		Aliases: []string{"rm"},
		Args:    cobra.NoArgs,
	}

	var subCmd *cobra.Command

	// vmc remove storagenode
	var storageNodeID uint32
	subCmd = newSNCmd()
	subCmd.Flags().Uint32Var(&storageNodeID, "storage-node-id", 0, "storage node identifier")
	subCmd.MarkFlagRequired("storage-node-id")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.removeStorageNode(types.StorageNodeID(storageNodeID))
	}
	cmd.AddCommand(subCmd)

	// vmc remove logstream
	var logStreamID uint32
	subCmd = newLSCmd()
	subCmd.Flags().Uint32Var(&logStreamID, "log-stream-id", 0, "log stream identifier")
	subCmd.MarkFlagRequired("log-stream-id")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.removeLogStream(types.LogStreamID(logStreamID))
	}
	cmd.AddCommand(subCmd)

	return cmd
}

func (app *VMCApp) initUpdateCmd() *cobra.Command {
	// vmc update
	cmd := &cobra.Command{
		Use:     "update",
		Aliases: []string{"mv"},
		Args:    cobra.NoArgs,
	}

	// vmc update logstream
	var (
		logStreamID       uint32
		popStorageNodeID  uint32
		pushStorageNodeID uint32
		pushPath          string
	)

	subCmd := newLSCmd()
	subCmd.Flags().Uint32Var(&logStreamID, "log-stream-id", 0, "log stream identifier")
	subCmd.MarkFlagRequired("log-stream-id")
	subCmd.Flags().Uint32Var(&popStorageNodeID, "pop-storage-node-id", 0, "storage node identifier")
	subCmd.Flags().Uint32Var(&pushStorageNodeID, "push-storage-node-id", 0, "storage node identifier")
	subCmd.Flags().StringVar(&pushPath, "push-storage-path", "", "storage node path")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		var (
			popReplica  *varlogpb.ReplicaDescriptor
			pushReplica *varlogpb.ReplicaDescriptor
		)
		if viper.IsSet("pop-storage-node-id") {
			popReplica = &varlogpb.ReplicaDescriptor{
				StorageNodeID: types.StorageNodeID(popStorageNodeID),
			}
		}
		if viper.IsSet("push-storage-node-id") && viper.IsSet("push-storage-path") {
			pushReplica = &varlogpb.ReplicaDescriptor{
				StorageNodeID: types.StorageNodeID(pushStorageNodeID),
				Path:          pushPath,
			}
		}
		app.updateLogStream(types.LogStreamID(logStreamID), popReplica, pushReplica)
	}
	cmd.AddCommand(subCmd)
	return cmd
}

func (app *VMCApp) initSealCmd() *cobra.Command {
	// vmc seal lotstream
	cmd := &cobra.Command{
		Use:  "seal",
		Args: cobra.NoArgs,
	}
	var logStreamID uint32
	subCmd := newLSCmd()
	subCmd.Flags().Uint32Var(&logStreamID, "log-stream-id", 0, "log stream identifier")
	subCmd.MarkFlagRequired("log-stream-id")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.sealLogStream(types.LogStreamID(logStreamID))
	}
	cmd.AddCommand(subCmd)
	return cmd
}

func (app *VMCApp) initUnsealCmd() *cobra.Command {
	// vmc unseal lotstream
	cmd := &cobra.Command{
		Use:  "unseal",
		Args: cobra.NoArgs,
	}
	var logStreamID uint32
	subCmd := newLSCmd()
	subCmd.Flags().Uint32Var(&logStreamID, "log-stream-id", 0, "log stream identifier")
	subCmd.MarkFlagRequired("log-stream-id")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.unsealLogStream(types.LogStreamID(logStreamID))
	}
	cmd.AddCommand(subCmd)
	return cmd
}

func (app *VMCApp) initSyncCmd() *cobra.Command {
	// vmc sync lotstream
	cmd := &cobra.Command{
		Use:  "sync",
		Args: cobra.NoArgs,
	}
	var (
		logStreamID      uint32
		srcStorageNodeID uint32
		dstStorageNodeID uint32
	)
	subCmd := newLSCmd()
	subCmd.Flags().Uint32Var(&logStreamID, "log-stream-id", 0, "log stream identifier")
	subCmd.Flags().Uint32("source-storage-node-id", 0, "source storage node identifier")
	subCmd.Flags().Uint32("destination-storage-node-id", 0, "destination storage node identifier")
	subCmd.MarkFlagRequired("log-stream-id")
	subCmd.Run = func(cmd *cobra.Command, args []string) {
		app.syncLogStream(types.LogStreamID(logStreamID), types.StorageNodeID(srcStorageNodeID), types.StorageNodeID(dstStorageNodeID))
	}
	cmd.AddCommand(subCmd)
	return cmd
}

func (app *VMCApp) initMetaCmd() *cobra.Command {
	// vmc metadata
	cmd := &cobra.Command{
		Use:     "metadata",
		Aliases: []string{"meta"},
		Args:    cobra.NoArgs,
	}

	var subCmd *cobra.Command

	// vmc metdata storagenode
	subCmd = newSNCmd()
	addStorageNodeIDFlag(subCmd.Flags())
	cmd.AddCommand(subCmd)

	// vmc metdata logstream
	subCmd = newLSCmd()
	addLogStreamIDFlag(subCmd.Flags())
	cmd.AddCommand(subCmd)

	// vmc metdata metadatarepository
	subCmd = newMRCmd()
	cmd.AddCommand(subCmd)

	// vmc metadata cluster
	subCmd = &cobra.Command{
		Use:  "cluster",
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(subCmd)

	return cmd
}

func (app *VMCApp) initMRMgmtCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "metadatarepository",
		Aliases: []string{"mr"},
	}

	infoCmd := &cobra.Command{
		Use:  "info",
		Args: cobra.NoArgs,
	}
	infoCmd.Run = func(cmd *cobra.Command, args []string) {
		app.infoMRMembers()
	}

	rmCmd := &cobra.Command{
		Use:  "remove",
		Args: cobra.NoArgs,
	}

	cmd.AddCommand(infoCmd, rmCmd)
	return cmd
}

func addStorageNodeIDFlag(flags *pflag.FlagSet) {
	flags.String("storage-node-id", "", "storage node identifier")
}

func addLogStreamIDFlag(flags *pflag.FlagSet) {
	flags.String("log-stream-id", "", "log stream identifier")
}

func newSNCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "storagenode",
		Aliases: []string{"sn"},
		Args:    cobra.NoArgs,
	}
}

func newLSCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "logstream",
		Aliases: []string{"ls"},
		Args:    cobra.NoArgs,
	}
}

func newMRCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "metadatarepository",
		Aliases: []string{"mr"},
		Args:    cobra.NoArgs,
	}
}
